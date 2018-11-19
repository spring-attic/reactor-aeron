package reactor.ipc.aeron.server;

import io.aeron.driver.AeronResources;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.OnDisposable;
import reactor.util.Logger;
import reactor.util.Loggers;

final class ServerHandler implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = Loggers.getLogger(ServerHandler.class);

  private final String category;

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final AeronOptions options;

  private final AeronResources aeronResources;

  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final HeartbeatWatchdog heartbeatWatchdog;

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  private final HeartbeatSender heartbeatSender;

  private final io.aeron.Subscription controlSubscription;

  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  private final DirectProcessor<Connection> connections = DirectProcessor.create();
  private final FluxSink<Connection> connectionSink = connections.sink();

  ServerHandler(String category, AeronResources aeronResources, AeronOptions options) {
    this.aeronResources = aeronResources;
    this.controlSubscription =
        aeronResources.controlSubscription(
            category,
            options.serverChannel(),
            options.serverStreamId(),
            "to receive control requests on",
            0,
            this);

    this.category = category;
    this.options = options;
    this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), category);
    this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), category);

    this.onClose
        .doOnTerminate(this::dispose0)
        .subscribe(null, th -> logger.warn("ServerHandler disposed with error: {}", th));
  }

  Flux<Connection> connections() {
    return connections.onBackpressureBuffer();
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
    logger.debug(
        "[{}] Received {} for connectRequestId: {}, channel={}, "
            + "clientControlStreamId={}, clientSessionStreamId={}",
        category,
        MessageType.CONNECT,
        connectRequestId,
        AeronUtils.minifyChannel(clientChannel),
        clientControlStreamId,
        clientSessionStreamId);

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
        .subscribeOn(Schedulers.newSingle("ds"))
        .subscribe(
            connectionSink::next,
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
  public void onHeartbeat(long sessionId) {
    heartbeatWatchdog.heartbeatReceived(sessionId);
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
      this.outbound = new DefaultAeronOutbound(category, aeronResources, clientChannel, options);
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
              options,
              heartbeatSender);

      this.onClose
          .doOnTerminate(this::dispose0)
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: {}", th));
    }

    Mono<? extends Connection> initialise() {

      return connector
          .connect()
          .then(outbound.initialise(sessionId, clientSessionStreamId))
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
                heartbeatWatchdog.add(
                    sessionId,
                    () -> {
                      heartbeatWatchdog.remove(sessionId);
                      dispose();
                    },
                    inbound::lastSignalTimeNs);

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

    private void dispose0() {
      handlers.remove(this);
      heartbeatWatchdog.remove(sessionId);
      connector.dispose();
      outbound.dispose();
      inbound.dispose();

      logger.debug("[{}] Closed session with sessionId: {}", category, sessionId);
    }
  }
}
