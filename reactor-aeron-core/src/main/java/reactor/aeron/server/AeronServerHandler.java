package reactor.aeron.server;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.ControlMessageSubscriber;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.OnDisposable;
import reactor.aeron.Protocol;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

final class AeronServerHandler implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = Loggers.getLogger(AeronServerHandler.class);

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final String category;
  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;
  private final String serverChannel;

  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronServerSettings settings) {
    category = settings.name();
    options = settings.options();
    resources = settings.aeronResources();
    handler = settings.handler();
    serverChannel = options.serverChannel();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, th -> logger.warn("AeronServerHandler disposed with error: " + th));
  }

  @Override
  public void onSubscription(Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnect(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Received {} for connectRequestId: {}, channel={}, "
              + "clientControlStreamId={}, clientSessionStreamId={}",
          category,
          MessageType.CONNECT,
          connectRequestId,
          AeronUtils.minifyChannel(clientChannel),
          clientControlStreamId,
          clientSessionStreamId);
    }

    int serverSessionStreamId = streamIdCounter.incrementAndGet();
    long sessionId = nextSessionId.incrementAndGet();
    SessionHandler sessionHandler =
        new SessionHandler(
            clientChannel,
            clientSessionStreamId,
            clientControlStreamId,
            connectRequestId,
            sessionId,
            serverSessionStreamId);

    sessionHandler
        .start()
        .subscribe(
            connection ->
                handler
                    .apply(connection) //
                    .subscribe(connection.disposeSubscriber()),
            th ->
                logger.error(
                    "[{}] Occurred exception on connect to {}, sessionId: {}, "
                        + "connectRequestId: {}, clientSessionStreamId: {}, "
                        + "clientControlStreamId: {}, serverSessionStreamId: {}, error: ",
                    category,
                    clientChannel,
                    sessionId,
                    connectRequestId,
                    clientSessionStreamId,
                    clientControlStreamId,
                    serverSessionStreamId,
                    th));
  }

  @Override
  public void onConnectAck(long connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.error(
        "[{}] Received unsupported server request {}, connectRequestId: {}",
        category,
        MessageType.CONNECT_ACK,
        connectRequestId);
  }

  @Override
  public void onDisconnect(long sessionId) {
    logger.info("[{}] Received {} for sessionId: {}", category, MessageType.DISCONNECT, sessionId);
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(SessionHandler::dispose);
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.defer(
        () ->
            Mono.whenDelayError(
                handlers
                    .stream()
                    .map(
                        sessionHandler -> {
                          sessionHandler.dispose();
                          return sessionHandler.onDispose();
                        })
                    .collect(Collectors.toList())));
  }

  private class SessionHandler implements Connection {

    private final Logger logger = Loggers.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;
    private final DefaultAeronInbound inbound;
    private final String clientChannel;
    private final int clientSessionStreamId;
    private final int serverSessionStreamId;
    private final long connectRequestId;
    private final long sessionId;

    private final Mono<MessagePublication> controlPublication;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private SessionHandler(
        String clientChannel,
        int clientSessionStreamId,
        int clientControlStreamId,
        long connectRequestId,
        long sessionId,
        int serverSessionStreamId) {
      this.clientSessionStreamId = clientSessionStreamId;
      this.clientChannel = clientChannel;
      this.outbound = new DefaultAeronOutbound(category, clientChannel, resources, options);
      this.connectRequestId = connectRequestId;
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
      this.inbound = new DefaultAeronInbound(category, resources);

      this.controlPublication =
          Mono.defer(() -> newControlPublication(clientChannel, clientControlStreamId)).cache();

      this.dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: " + th));
    }

    private Mono<MessagePublication> newControlPublication(
        String clientChannel, int clientControlStreamId) {
      return resources.messagePublication(
          category, clientChannel, clientControlStreamId, options, resources.nextEventLoop());
    }

    private Mono<? extends Connection> start() {
      return connect()
          .then(outbound.start(sessionId, clientSessionStreamId))
          .then(inbound.start(serverChannel, serverSessionStreamId, sessionId, this::dispose))
          .thenReturn(this)
          .doOnSuccess(
              connection -> {
                handlers.add(this);

                logger.debug(
                    "[{}] Client with connectRequestId: {} successfully connected, sessionId: {}",
                    category,
                    connectRequestId,
                    sessionId);
              })
          .doOnError(
              th -> {
                logger.debug(
                    "[{}] Failed to connect to the client for sessionId: {}",
                    category,
                    sessionId,
                    th);

                dispose();
              });
    }

    @Override
    public AeronInbound inbound() {
      return inbound;
    }

    @Override
    public AeronOutbound outbound() {
      return outbound;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ServerSession{");
      sb.append("sessionId=").append(sessionId);
      sb.append(", clientChannel=").append(clientChannel);
      sb.append(", clientSessionStreamId=").append(clientSessionStreamId);
      sb.append(", serverSessionStreamId=").append(serverSessionStreamId);
      sb.append(", connectRequestId=").append(connectRequestId);
      sb.append('}');
      return sb.toString();
    }

    @Override
    public void dispose() {
      dispose.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return onDispose.isDisposed();
    }

    @Override
    public Mono<Void> onDispose() {
      return onDispose;
    }

    private Mono<Void> doDispose() {
      return Mono.defer(
          () -> {
            logger.debug("[{}] About to close session with sessionId: {}", category, sessionId);

            handlers.remove(this);

            Optional.ofNullable(outbound) //
                .ifPresent(DefaultAeronOutbound::dispose);
            Optional.ofNullable(inbound) //
                .ifPresent(DefaultAeronInbound::dispose);

            return Mono.whenDelayError(
                    Optional.ofNullable(outbound)
                        .map(DefaultAeronOutbound::onDispose)
                        .orElse(Mono.empty()),
                    Optional.ofNullable(inbound)
                        .map(DefaultAeronInbound::onDispose)
                        .orElse(Mono.empty()))
                .doFinally(
                    s ->
                        logger.debug(
                            "[{}] Closed session with sessionId: {}", category, sessionId));
          });
    }

    private Mono<Void> connect() {
      return Mono.defer(
          () -> {
            Duration retryInterval = Duration.ofMillis(100);
            Duration connectTimeout = options.connectTimeout().plus(options.backpressureTimeout());
            long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

            return controlPublication.flatMap(
                publication ->
                    sendConnectAck(publication)
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .doOnSuccess(
                            avoid ->
                                logger.debug(
                                    "[{}] Sent {} to {}",
                                    category,
                                    MessageType.CONNECT_ACK,
                                    publication))
                        .onErrorResume(
                            throwable -> {
                              String errMessage =
                                  String.format(
                                      "Failed to send %s, publication %s is not connected",
                                      MessageType.CONNECT_ACK, publication);
                              return Mono.error(new RuntimeException(errMessage, throwable));
                            }));
          });
    }

    private Mono<Void> sendConnectAck(MessagePublication publication) {
      return publication.enqueue(
          Protocol.createConnectAckBody(sessionId, connectRequestId, serverSessionStreamId));
    }
  }
}
