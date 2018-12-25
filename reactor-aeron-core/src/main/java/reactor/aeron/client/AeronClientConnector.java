package reactor.aeron.client;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AeronClientConnector implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = Loggers.getLogger(AeronClientConnector.class);

  private static final int CONTROL_STREAM_ID = 1;

  private static final AtomicLong connectRequestIdCounter =
      new AtomicLong(System.currentTimeMillis());

  private final String category;
  private final AeronOptions options;
  private final AeronResources resources;
  private final int clientControlStreamId;
  private final String clientChannel;
  private final Supplier<Integer> clientSessionStreamIdCounter;

  private final Map<Long, ConnectAckPromise> connectAckPromises = new ConcurrentHashMap<>();

  private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronClientConnector(
      AeronClientSettings settings,
      int clientControlStreamId,
      Supplier<Integer> clientSessionStreamIdCounter) {
    options = settings.options();
    category = Optional.ofNullable(settings.name()).orElse("client");
    resources = settings.aeronResources();
    clientChannel = options.clientChannel();
    this.clientControlStreamId = clientControlStreamId;
    this.clientSessionStreamIdCounter = clientSessionStreamIdCounter;

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, th -> logger.warn("AeronClientConnector disposed with error: " + th));
  }

  /**
   * Creates ClientHandler object and starts it. See {@link ClientHandler#start()}.
   *
   * @return a {@link Mono} completing with a {@link Disposable} token to dispose the active handler
   *     (server, client connection...) or failing with the connection error.
   */
  public Mono<Connection> start() {
    return Mono.defer(() -> new ClientHandler().start());
  }

  private void dispose(long sessionId) {
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(ClientHandler::dispose);
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

  private class ClientHandler implements Connection {

    private final DefaultAeronOutbound outbound;
    private final int clientSessionStreamId;
    private final long connectRequestId = connectRequestIdCounter.incrementAndGet();
    private final String serverChannel;
    private final Mono<MessagePublication> controlPublication;

    private volatile long sessionId;
    private volatile int serverSessionStreamId;
    private volatile DefaultAeronInbound inbound;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private ClientHandler() {
      clientSessionStreamId = clientSessionStreamIdCounter.get();
      serverChannel = options.serverChannel();
      inbound = new DefaultAeronInbound(category, resources);
      outbound = new DefaultAeronOutbound(category, serverChannel, resources, options);

      controlPublication = Mono.defer(this::newControlPublication).cache();

      dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(null, th -> logger.warn("ClientHandler disposed with error: " + th));
    }

    private Mono<MessagePublication> newControlPublication() {
      return resources.messagePublication(
          category, serverChannel, CONTROL_STREAM_ID, options, resources.nextEventLoop());
    }

    private Mono<? extends Connection> start() {
      handlers.add(this);

      return connect()
          .flatMap(
              response -> {
                sessionId = response.sessionId;
                serverSessionStreamId = response.serverSessionStreamId;

                return inbound
                    .start(clientChannel, clientSessionStreamId, sessionId, this::dispose)
                    .then(outbound.start(sessionId, serverSessionStreamId))
                    .thenReturn(this);
              })
          .doOnError(
              ex -> {
                logger.error(
                    "[{}] Occurred exception for sessionId: {} clientSessionStreamId: {}, error: ",
                    category,
                    sessionId,
                    clientSessionStreamId,
                    ex);

                dispose();
              })
          .thenReturn(this);
    }

    private Mono<ConnectAckResponse> connect() {
      ConnectAckPromise connectAckPromise =
          connectAckPromises.computeIfAbsent(connectRequestId, ConnectAckPromise::new);

      return sendConnectRequest()
          .then(
              connectAckPromise
                  .promise()
                  .timeout(options.ackTimeout())
                  .doOnError(
                      ex ->
                          logger.warn(
                              "Failed to receive {} during {} millis",
                              MessageType.CONNECT_ACK,
                              options.ackTimeout().toMillis())))
          .doOnSuccess(
              response ->
                  logger.debug(
                      "[{}] Successfully connected to server at {}, sessionId: {}",
                      category,
                      AeronUtils.minifyChannel(serverChannel),
                      response.sessionId))
          .doOnTerminate(connectAckPromise::dispose)
          .doOnError(
              ex ->
                  logger.warn(
                      "Failed to connect to server at {}, cause: {}",
                      AeronUtils.minifyChannel(serverChannel),
                      ex.toString()));
    }

    private Mono<Void> sendConnectRequest() {
      return controlPublication.flatMap(
          publication -> {
            logger.debug(
                "[{}] Connecting to server at {}",
                category,
                AeronUtils.minifyChannel(serverChannel));

            return send(
                publication,
                Protocol.createConnectBody(
                    connectRequestId, clientChannel, clientControlStreamId, clientSessionStreamId),
                MessageType.CONNECT);
          });
    }

    private Mono<Void> sendDisconnectRequest() {
      return controlPublication.flatMap(
          publication -> {
            logger.debug(
                "[{}] Disconnecting from server at {}",
                category,
                AeronUtils.minifyChannel(serverChannel));

            return send(
                publication, Protocol.createDisconnectBody(sessionId), MessageType.COMPLETE);
          });
    }

    private Mono<Void> send(MessagePublication mp, ByteBuffer buffer, MessageType messageType) {
      return mp.enqueue(buffer)
          .doOnSuccess(
              avoid ->
                  logger.debug(
                      "[{}] Sent {} to {}",
                      category,
                      messageType,
                      AeronUtils.minifyChannel(serverChannel)))
          .doOnError(
              ex ->
                  logger.warn(
                      "[{}] Failed to send {} to {}, cause: {}",
                      category,
                      messageType,
                      AeronUtils.minifyChannel(serverChannel),
                      ex.toString()));
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
      final StringBuilder sb = new StringBuilder("ClientSession{");
      sb.append("sessionId=").append(sessionId);
      sb.append(", clientChannel=").append(clientChannel);
      sb.append(", serverChannel=").append(serverChannel);
      sb.append(", clientSessionStreamId=").append(clientSessionStreamId);
      sb.append(", serverSessionStreamId=").append(serverSessionStreamId);
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

            return sendDisconnectRequest()
                .onErrorResume(ex -> Mono.empty())
                .then(
                    Mono.defer(
                        () -> {
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
                                          "[{}] Closed session with sessionId: {}",
                                          category,
                                          sessionId));
                        }));
          });
    }
  }

  @Override
  public void onSubscription(org.reactivestreams.Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnectAck(long connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.debug(
        "[{}] Received {} for connectRequestId: {}, serverSessionStreamId: {}",
        category,
        MessageType.CONNECT_ACK,
        connectRequestId,
        serverSessionStreamId);

    ConnectAckPromise connectAckPromise = connectAckPromises.remove(connectRequestId);
    if (connectAckPromise != null) {
      connectAckPromise.success(sessionId, serverSessionStreamId);
    }
  }

  /**
   * Handler for complete signal from server. At the moment of writing this javadoc the server
   * doesn't emit complete signal. Method is left with logging.
   *
   * <p>See for details: {@link MessageType#COMPLETE}, {@link Protocol#createDisconnectBody(long)}.
   *
   * @param sessionId session id
   */
  @Override
  public void onComplete(long sessionId) {
    logger.info("[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
    dispose(sessionId);
  }

  @Override
  public void onConnect(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {
    logger.error(
        "[{}] Unsupported {} request for a client, clientChannel: {}, "
            + "clientControlStreamId: {}, clientSessionStreamId: {}",
        category,
        MessageType.CONNECT,
        clientChannel,
        clientControlStreamId,
        clientSessionStreamId);
  }

  /**
   * ConnectAck promise. Holds pair of connect request UUID and associated MonoProcessor. Clients
   * use method {@link #promise()} to subscribe. When server responds then method {@link
   * #success(long, int)} is being called.
   */
  private class ConnectAckPromise implements Disposable {

    private final long connectRequestId;
    private final MonoProcessor<ConnectAckResponse> promise;

    private ConnectAckPromise(long connectRequestId) {
      this.connectRequestId = connectRequestId;
      this.promise = MonoProcessor.create();
    }

    private Mono<ConnectAckResponse> promise() {
      return promise;
    }

    private void success(long sessionId, int serverSessionStreamId) {
      promise.onNext(new ConnectAckResponse(sessionId, serverSessionStreamId));
      promise.onComplete();
    }

    @Override
    public void dispose() {
      connectAckPromises.remove(connectRequestId);
      promise.cancel();
    }

    @Override
    public boolean isDisposed() {
      return promise.isDisposed();
    }
  }

  /**
   * Tuple containing connection ack response from server which are: session id and server session
   * stream id. DTO class.
   */
  private static class ConnectAckResponse {

    private final long sessionId;
    private final int serverSessionStreamId;

    private ConnectAckResponse(long sessionId, int serverSessionStreamId) {
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
    }
  }
}
