package reactor.aeron.client;

import io.aeron.Image;
import io.aeron.Subscription;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.HeartbeatSender;
import reactor.aeron.HeartbeatWatchdog;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AeronClientConnector implements Disposable {

  private static final Logger logger = Loggers.getLogger(AeronClientConnector.class);

  private static final int CONTROL_SESSION_ID = 0;

  private final String name;
  private final AeronClientOptions options;
  private final AeronResources aeronResources;

  private static final AtomicInteger streamIdCounter = new AtomicInteger();

  private final ClientControlMessageSubscriber controlMessageSubscriber;

  private final Subscription controlSubscription;

  private final int clientControlStreamId;

  private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

  AeronClientConnector(AeronClientSettings settings) {
    this.options = settings.options();
    this.name = Optional.ofNullable(settings.name()).orElse("client");
    this.aeronResources = settings.aeronResources();
    this.controlMessageSubscriber =
        new ClientControlMessageSubscriber(name, this::dispose);
    this.clientControlStreamId = streamIdCounter.incrementAndGet();
    this.controlSubscription =
        aeronResources.controlSubscription(
            name,
            options.clientChannel(),
            clientControlStreamId,
            "to receive control requests on",
            CONTROL_SESSION_ID,
            controlMessageSubscriber,
            null,
            this::onUnavailableControlImage);
  }

  private void onUnavailableControlImage(Image image) {
    if (image.subscription() == controlSubscription && controlSubscription.hasNoImages()) {
      dispose();
    }
  }

  /**
   * Create a new handler.
   *
   * @return a {@link Mono} completing with a {@link Disposable} token to dispose the active handler
   *     (server, client connection...) or failing with the connection error.
   */
  public Mono<Connection> newHandler() {
    return new ClientHandler().initialise();
  }

  @Override
  public void dispose() {
    handlers.forEach(ClientHandler::dispose);
    handlers.clear();
    aeronResources.close(controlSubscription);
  }

  private void dispose(long sessionId) {
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(ClientHandler::dispose);
  }

  class ClientHandler implements Connection {

    private final DefaultAeronOutbound outbound;

    private final int clientSessionStreamId;

    private final ClientConnector connector;

    private volatile AeronClientInbound inbound;

    private volatile long sessionId;

    private volatile int serverSessionStreamId;

    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    ClientHandler() {
      this.clientSessionStreamId = streamIdCounter.incrementAndGet();
      this.outbound =
          new DefaultAeronOutbound(name, aeronResources, options.serverChannel(), options);
      this.connector =
          new ClientConnector(
              name,
              aeronResources,
              options,
              controlMessageSubscriber,
              clientControlStreamId,
              clientSessionStreamId);

      this.onClose
          .doOnTerminate(this::dispose0)
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: {}", th));
    }

    Mono<Connection> initialise() {
      handlers.add(this);

      return connector
          .connect()
          .flatMap(
              connectAckResponse -> {
                this.sessionId = connectAckResponse.sessionId;
                this.serverSessionStreamId = connectAckResponse.serverSessionStreamId;

                inbound =
                    new AeronClientInbound(
                        name,
                        aeronResources,
                        options.clientChannel(),
                        clientSessionStreamId,
                        sessionId,
                        this::dispose);

                return outbound.initialise(sessionId, serverSessionStreamId);
              })
          .doOnError(
              th -> {
                logger.error(
                    "[{}] Occurred exception for sessionId: {} clientSessionStreamId: {}, error: ",
                    name,
                    sessionId,
                    clientSessionStreamId,
                    th);
                dispose();
              })
          .then(Mono.just(this));
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
      final StringBuilder sb = new StringBuilder("ClientSession{");
      sb.append("sessionId=").append(sessionId);
      sb.append(", clientChannel=").append(options.clientChannel());
      sb.append(", serverChannel=").append(options.serverChannel());
      sb.append(", clientSessionStreamId=").append(clientSessionStreamId);
      sb.append(", serverSessionStreamId=").append(serverSessionStreamId);
      sb.append('}');
      return sb.toString();
    }

    public void dispose0() {
      handlers.remove(this);
      connector.dispose();
      if (inbound != null) {
        inbound.dispose();
      }
      outbound.dispose();

      logger.debug("[{}] Closed session with Id: {}", name, sessionId);
    }
  }
}
