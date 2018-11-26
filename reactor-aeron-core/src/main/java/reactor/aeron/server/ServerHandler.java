package reactor.aeron.server;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Subscription;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.ControlMessageSubscriber;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessageType;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

final class ServerHandler implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = Loggers.getLogger(ServerHandler.class);

  private static final int CONTROL_SESSION_ID = 0;

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final AeronServerSettings settings;
  private final String category;
  private final AeronServerOptions options;
  private final AeronResources aeronResources;

  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  private final io.aeron.Subscription controlSubscription;

  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  ServerHandler(AeronServerSettings settings) {
    this.settings = settings;
    this.category = settings.name();
    this.options = settings.options();
    this.aeronResources = settings.aeronResources();

    this.controlSubscription =
        settings
            .aeronResources()
            .controlSubscription(
                category,
                options.serverChannel(),
                options.controlStreamId(),
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
    aeronResources.close(controlSubscription);
  }

  class SessionHandler implements Connection {

    private final Logger logger = Loggers.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;

    private final AeronServerInbound inbound;

    private final String clientChannel;

    private final int clientSessionStreamId;

    private final int serverSessionStreamId;

    private final UUID connectRequestId;

    private final long sessionId;

    private final ServerConnector connector;

    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    SessionHandler(
        String clientChannel,
        int clientSessionStreamId,
        int clientControlStreamId,
        UUID connectRequestId,
        long sessionId,
        int serverSessionStreamId) {
      this.clientSessionStreamId = clientSessionStreamId;
      this.clientChannel = clientChannel;
      this.outbound = new DefaultAeronOutbound(category, aeronResources, clientChannel);
      this.connectRequestId = connectRequestId;
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
      this.inbound = new AeronServerInbound(category, aeronResources);
      this.connector =
          new ServerConnector(
              category,
              aeronResources,
              clientChannel,
              clientControlStreamId,
              sessionId,
              serverSessionStreamId,
              connectRequestId,
              options);

      this.onClose
          .doOnTerminate(this::dispose0)
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: {}", th));
    }

    Mono<? extends Connection> initialise() {

      return connector
          .connect()
          .then(outbound.initialise(sessionId, clientSessionStreamId, options))
          .then(
              inbound.initialise(
                  category,
                  options.serverChannel(),
                  serverSessionStreamId,
                  sessionId,
                  this::dispose))
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
    public void dispose() {
      if (!onClose.isDisposed()) {
        onClose.onComplete();
      }
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
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

    private void dispose0() {
      handlers.remove(this);
      connector.dispose();
      outbound.dispose();
      inbound.dispose();

      logger.debug("[{}] Closed session with sessionId: {}", category, sessionId);
    }
  }
}
