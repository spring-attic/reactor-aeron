package reactor.aeron.client;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import io.aeron.Image;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.Protocol;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AeronClientConnector implements Disposable {

  private static final Logger logger = Loggers.getLogger(AeronClientConnector.class);

  private static final int CONTROL_SESSION_ID = 0;
  private static final int CONTROL_STREAM_ID = 1;

  private static final TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator();

  private static final AtomicInteger streamIdCounter = new AtomicInteger();

  private final String name;
  private final AeronOptions options;
  private final AeronResources resources;
  private final int clientControlStreamId;
  private final String clientChannel;

  private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

  private final io.aeron.Subscription controlSubscription;
  private final ClientControlMessageSubscriber controlMessageSubscriber;

  AeronClientConnector(AeronClientSettings settings) {
    this.options = settings.options();
    this.name = Optional.ofNullable(settings.name()).orElse("client");
    this.resources = settings.aeronResources();
    this.controlMessageSubscriber = new ClientControlMessageSubscriber(name, this::dispose);
    this.clientControlStreamId = streamIdCounter.incrementAndGet();
    clientChannel = options.clientChannel();
    this.controlSubscription =
        resources.controlSubscription(
            name,
            clientChannel,
            clientControlStreamId,
            controlMessageSubscriber,
            null,
            this::onUnavailableControlImage);
  }

  private void onUnavailableControlImage(Image image) {
    if (controlSubscription.hasNoImages()) {
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
    resources.close(controlSubscription);
  }

  private void dispose(long sessionId) {
    handlers
        .stream()
        .filter(handler -> handler.sessionId == sessionId)
        .findFirst()
        .ifPresent(ClientHandler::dispose);
  }

  private class ClientHandler implements Connection {

    private final DefaultAeronOutbound outbound;
    private final int clientSessionStreamId;
    private final UUID connectRequestId = uuidGenerator.generate();
    private final String serverChannel;
    private final Mono<MessagePublication> controlPublication;

    private volatile long sessionId;
    private volatile int serverSessionStreamId;
    private volatile AeronClientInbound inbound;

    // TODO refactor OnDispose mechanism
    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    private ClientHandler() {
      this.clientSessionStreamId = streamIdCounter.incrementAndGet();
      this.serverChannel = options.serverChannel();

      this.outbound = //
          new DefaultAeronOutbound(name, serverChannel, resources, options);

      this.onClose
          .doOnTerminate(this::dispose0)
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: {}", th));

      this.controlPublication = Mono.defer(this::newControlPublication).cache();
    }

    private Mono<MessagePublication> newControlPublication() {
      return resources.messagePublication(
          name, serverChannel, CONTROL_STREAM_ID, options, resources.nextEventLoop());
    }

    Mono<Connection> initialise() {
      handlers.add(this);

      return connect()
          .flatMap(
              connectAckResponse -> {
                this.sessionId = connectAckResponse.sessionId;
                this.serverSessionStreamId = connectAckResponse.serverSessionStreamId;

                inbound =
                    new AeronClientInbound(
                        name,
                        resources,
                        clientChannel,
                        clientSessionStreamId,
                        sessionId,
                        this::dispose);

                return outbound.initialise(sessionId, serverSessionStreamId);
              })
          .doOnError(
              ex -> {
                logger.error(
                    "[{}] Occurred exception for sessionId: {} clientSessionStreamId: {}, error: ",
                    name,
                    sessionId,
                    clientSessionStreamId,
                    ex);
                dispose();
              })
          .thenReturn(this);
    }

    private Mono<ClientControlMessageSubscriber.ConnectAckResponse> connect() {
      ClientControlMessageSubscriber.ConnectAckSubscription connectAckSubscription =
          controlMessageSubscriber.subscribeForConnectAck(connectRequestId);

      return sendConnectRequest()
          .then(
              connectAckSubscription
                  .connectAck()
                  .timeout(options.ackTimeout())
                  .doOnError(
                      ex ->
                          logger.warn(
                              "Failed to receive {} during {} millis",
                              MessageType.CONNECT_ACK,
                              options.ackTimeout().toMillis())))
          .doOnSuccess(
              response -> {
                this.sessionId = response.sessionId;
                logger.debug(
                    "[{}] Successfully connected to server at {}, sessionId: {}",
                    name,
                    AeronUtils.minifyChannel(serverChannel),
                    sessionId);
              })
          .doOnTerminate(connectAckSubscription::dispose)
          .doOnError(
              ex ->
                  logger.warn(
                      "Failed to connect to server at {}, cause: {}",
                      AeronUtils.minifyChannel(serverChannel),
                      ex));
    }

    private Mono<Void> sendConnectRequest() {
      return controlPublication.flatMap(
          publication -> {
            logger.debug(
                "[{}] Connecting to server at {}", name, AeronUtils.minifyChannel(serverChannel));

            ByteBuffer buffer =
                Protocol.createConnectBody(
                    connectRequestId, clientChannel, clientControlStreamId, clientSessionStreamId);

            return send(publication, buffer, MessageType.CONNECT);
          });
    }

    private Mono<Void> sendDisconnectRequest() {
      return controlPublication.flatMap(
          publication -> {
            logger.debug(
                "[{}] Disconnecting from server at {}",
                name,
                AeronUtils.minifyChannel(serverChannel));

            ByteBuffer buffer = Protocol.createDisconnectBody(sessionId);

            return send(publication, buffer, MessageType.COMPLETE);
          });
    }

    private Mono<Void> send(MessagePublication mp, ByteBuffer buffer, MessageType messageType) {
      return mp.enqueue(messageType, buffer, sessionId)
          .doOnSuccess(
              avoid ->
                  logger.debug(
                      "[{}] Sent {} to {}",
                      name,
                      messageType,
                      AeronUtils.minifyChannel(serverChannel)))
          .doOnError(
              ex ->
                  logger.warn(
                      "[{}] Failed to send {} to {}, cause: {}",
                      name,
                      messageType,
                      AeronUtils.minifyChannel(serverChannel),
                      ex));
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
      if (!onClose.isDisposed()) {
        onClose.onComplete();
      }
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    public void dispose0() {
      handlers.remove(this);

      sendDisconnectRequest()
          .subscribe(
              null,
              th -> {
                // no-op
              });

      // TODO control subscription must be closed somehow

      if (inbound != null) {
        inbound.dispose();
      }
      if (outbound != null) {
        outbound.dispose();
      }

      logger.debug("[{}] Closed session: {}", name, sessionId);
    }
  }
}
