package reactor.aeron.server;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Subscription;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.ControlMessageSubscriber;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.OnDisposable;
import reactor.aeron.Protocol;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

final class ServerHandler implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = Loggers.getLogger(ServerHandler.class);

  private static final int CONTROL_SESSION_ID = 0;
  private static final int CONTROL_STREAM_ID = 1;

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final AeronServerSettings settings;
  private final String category;
  private final AeronOptions options;
  private final AeronResources resources;
  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final io.aeron.Subscription controlSubscription;

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  // TODO refactor OnDispose mechanism
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  ServerHandler(AeronServerSettings settings) {
    this.settings = settings;
    this.category = settings.name();
    this.options = settings.options();
    this.resources = settings.aeronResources();

    this.controlSubscription =
        resources.controlSubscription(
            category,
            options.serverChannel(),
            CONTROL_STREAM_ID,
            "to receive control requests on",
            CONTROL_SESSION_ID,
            this,
            null,
            null);

    this.onClose
        .doOnTerminate(this::dispose0)
        .subscribe(null, th -> logger.warn("ServerHandler disposed with error: {}", th));
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnect(
      UUID connectRequestId,
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
        .initialise()
        .subscribeOn(Schedulers.single())
        .subscribe(
            connection ->
                settings
                    .handler() //
                    .apply(connection)
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
  public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.error(
        "[{}] Received unsupported server request {}, connectRequestId: {}",
        category,
        MessageType.CONNECT_ACK,
        connectRequestId);
  }

  @Override
  public void onComplete(long sessionId) {
    logger.info("[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(SessionHandler::dispose);
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      onClose.onComplete();
    }
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onClose;
  }

  private void dispose0() {
    handlers.forEach(SessionHandler::dispose);
    handlers.clear();
    resources.close(controlSubscription);
  }

  private class SessionHandler implements Connection {

    private final Logger logger = Loggers.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;
    private final AeronServerInbound inbound;
    private final String clientChannel;
    private final int clientSessionStreamId;
    private final int serverSessionStreamId;
    private final UUID connectRequestId;
    private final long sessionId;

    private final Mono<MessagePublication> controlPublication;

    // TODO refactor OnDispose mechanism
    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    private SessionHandler(
        String clientChannel,
        int clientSessionStreamId,
        int clientControlStreamId,
        UUID connectRequestId,
        long sessionId,
        int serverSessionStreamId) {
      this.clientSessionStreamId = clientSessionStreamId;
      this.clientChannel = clientChannel;
      this.outbound = new DefaultAeronOutbound(category, clientChannel, resources, options);
      this.connectRequestId = connectRequestId;
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
      this.inbound = new AeronServerInbound(category, resources);

      this.controlPublication =
          Mono.defer(() -> newControlPublication(clientChannel, clientControlStreamId)).cache();

      this.onClose
          .doOnTerminate(this::dispose0)
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: {}", th));
    }

    private Mono<MessagePublication> newControlPublication(
        String clientChannel, int clientControlStreamId) {
      return resources.messagePublication(
          category, clientChannel, clientControlStreamId, options, resources.nextEventLoop());
    }

    Mono<? extends Connection> initialise() {

      return connect()
          .then(outbound.initialise(sessionId, clientSessionStreamId))
          .log("outbound ")
          .then(
              inbound.initialise(
                  category,
                  options.serverChannel(),
                  serverSessionStreamId,
                  sessionId,
                  this::dispose))
          .log("inbound ")
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
    public Mono<Void> onDispose() {
      return onClose;
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
      if (!onClose.isDisposed()) {
        onClose.onComplete();
      }
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    private void dispose0() {
      handlers.remove(this);

      // connector.dispose(); todo
      // === connector.dispose() ===== BEGING ===
      // resources.close(clientControlPublication);
      // === connector.dispose() ===== END ===

      outbound.dispose();
      inbound.dispose();

      logger.debug("[{}] Closed session with sessionId: {}", category, sessionId);
    }

    Mono<Void> connect() {
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
      ByteBuffer buffer = Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId);
      return publication.enqueue(MessageType.CONNECT_ACK, buffer, sessionId);
    }
  }
}
