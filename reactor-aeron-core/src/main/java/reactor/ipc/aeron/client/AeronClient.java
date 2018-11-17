package reactor.ipc.aeron.client;

import io.aeron.Subscription;
import io.aeron.driver.AeronResources;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AeronClient implements AeronConnector, Disposable {

  private static final Logger logger = Loggers.getLogger(AeronClient.class);

  private final AeronClientOptions options;

  private final String name;

  private static final AtomicInteger streamIdCounter = new AtomicInteger();

  private final ClientControlMessageSubscriber controlMessageSubscriber;

  private final Subscription controlSubscription;

  private final int clientControlStreamId;

  private final HeartbeatSender heartbeatSender;

  private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

  private final HeartbeatWatchdog heartbeatWatchdog;

  private final AeronResources aeronResources;

  /**
   * Create aeron client.
   *
   * @param aeronResources aeronResources
   * @return aeron client
   */
  public static AeronClient create(AeronResources aeronResources) {
    return new AeronClient(
        null,
        aeronResources,
        options -> {
          // no-op
        });
  }

  public static AeronClient create(
      String name, AeronResources aeronResources, Consumer<AeronClientOptions> optionsConfigurer) {
    return new AeronClient(name, aeronResources, optionsConfigurer);
  }

  private AeronClient(
      String name, AeronResources aeronResources, Consumer<AeronClientOptions> optionsConfigurer) {
    AeronClientOptions options = new AeronClientOptions();
    optionsConfigurer.accept(options);
    this.options = options;
    this.name = name == null ? "client" : name;
    this.aeronResources = aeronResources;
    this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), this.name);
    this.controlMessageSubscriber =
        new ClientControlMessageSubscriber(name, heartbeatWatchdog, this::dispose);
    this.clientControlStreamId = streamIdCounter.incrementAndGet();
    this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), this.name);
    this.controlSubscription =
        aeronResources.controlSubscription(
            name,
            options.clientChannel(),
            clientControlStreamId,
            "to receive control requests on",
            0,
            controlMessageSubscriber);
  }

  @Override
  public Mono<? extends Disposable> newHandler(
      BiFunction<AeronInbound, AeronOutbound, ? extends Publisher<Void>> ioHandler) {
    ClientHandler handler = new ClientHandler(ioHandler);
    return handler.initialise();
  }

  @Override
  public void dispose() {
    handlers.forEach(ClientHandler::dispose);
    handlers.clear();
    aeronResources.release(controlSubscription);
  }

  private void dispose(long sessionId) {
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(ClientHandler::dispose);
  }

  class ClientHandler implements Disposable {

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>>
        ioHandler;

    private final DefaultAeronOutbound outbound;

    private final int clientSessionStreamId;

    private final ClientConnector connector;

    private volatile AeronClientInbound inbound;

    private volatile long sessionId;

    ClientHandler(
        BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>>
            ioHandler) {
      this.ioHandler = ioHandler;
      this.clientSessionStreamId = streamIdCounter.incrementAndGet();
      this.outbound =
          new DefaultAeronOutbound(name, aeronResources, options.serverChannel(), options);
      this.connector =
          new ClientConnector(
              name,
              aeronResources,
              options,
              controlMessageSubscriber,
              heartbeatSender,
              clientControlStreamId,
              clientSessionStreamId);
    }

    Mono<Disposable> initialise() {
      handlers.add(this);

      return connector
          .connect()
          .flatMap(
              connectAckResponse -> {
                inbound =
                    new AeronClientInbound(
                        name,
                        aeronResources,
                        options.clientChannel(),
                        clientSessionStreamId,
                        connectAckResponse.sessionId,
                        this::dispose);

                this.sessionId = connectAckResponse.sessionId;
                heartbeatWatchdog.add(
                    connectAckResponse.sessionId,
                    this::dispose,
                    () -> inbound.getLastSignalTimeNs());

                return outbound.initialise(
                    connectAckResponse.sessionId, connectAckResponse.serverSessionStreamId);
              })
          .doOnSuccess(
              avoid ->
                  Mono.from(ioHandler.apply(inbound, outbound))
                      .doOnTerminate(this::dispose)
                      .subscribe())
          .doOnError(
              th -> {
                logger.error(
                    "[{}] Occurred exception for sessionId: {} clientSessionStreamId: {}, error: ",
                    name,
                    sessionId,
                    clientSessionStreamId);
                dispose();
              })
          .then(Mono.just(this));
    }

    @Override
    public void dispose() {
      connector.dispose();

      handlers.remove(this);

      heartbeatWatchdog.remove(sessionId);

      if (inbound != null) {
        inbound.dispose();
      }
      outbound.dispose();

      if (sessionId > 0) {
        logger.debug("[{}] Closed session with Id: {}", name, sessionId);
      }
    }
  }
}
