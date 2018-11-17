package reactor.ipc.aeron.server;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronResources;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.MessageType;
import reactor.util.Logger;
import reactor.util.Loggers;

final class ServerHandler implements ControlMessageSubscriber, Disposable {

  private static final Logger logger = Loggers.getLogger(ServerHandler.class);

  private final String category;

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>>
      ioHandler;

  private final AeronOptions options;

  private final AeronResources aeronResources;

  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final HeartbeatWatchdog heartbeatWatchdog;

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  private final HeartbeatSender heartbeatSender;

  private final io.aeron.Subscription controlSubscription;

  ServerHandler(
      String category,
      BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
      AeronResources aeronResources,
      AeronOptions options) {
    this.aeronResources = aeronResources;
    this.controlSubscription =
        aeronResources.controlSubscription(
            options.serverChannel(),
            options.serverStreamId(),
            "to receive control requests on",
            0,
            this);

    this.category = category;
    this.ioHandler = ioHandler;
    this.options = options;
    this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), category);
    this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), category);
  }

  @Override
  public void dispose() {
    handlers.forEach(SessionHandler::dispose);
    handlers.clear();
    aeronResources.release(controlSubscription);
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
            null,
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

  class SessionHandler implements Disposable {

    private final Logger logger = Loggers.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;

    private final AeronServerInbound inbound;

    private final int clientSessionStreamId;

    private final int serverSessionStreamId;

    private final UUID connectRequestId;

    private final long sessionId;

    private final ServerConnector connector;

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
    }

    Mono<Void> initialise() {

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
          .doOnSuccess(
              avoid -> {
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

                Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
                Mono.from(publisher).doOnTerminate(this::dispose).subscribe();
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
    public void dispose() {
      handlers.remove(this);

      heartbeatWatchdog.remove(sessionId);

      connector.dispose();

      outbound.dispose();
      inbound.dispose();

      logger.debug("[{}] Closed session with sessionId: {}", category, sessionId);
    }
  }
}
