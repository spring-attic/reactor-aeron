package reactor.aeron.server;

import io.aeron.Image;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronConnection;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessageSubscription;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron server handler. Schematically can be described as:
 *
 * <pre>
 * Server
 * serverPort->inbound->Sub(endpoint, acceptor[onImageAvailable, onImageUnavailbe])
 * + onImageAvailable(Image)
 * sessionId->inbound->EmitterPocessor
 * serverControlPort->outbound->MDC(sessionId)->Pub(control-endpoint, sessionId)
 * </pre>
 */
final class AeronServerHandler implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  private volatile MessageSubscription subscription; // server acceptor subscription

  private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();
    this.handler = options.handler();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  Mono<OnDisposable> start() {
    return Mono.defer(
        () -> {
          // Sub(endpoint{address:serverPort})
          String inboundChannel = options.inboundUri().asString();

          logger.debug("Starting {} on: {}", this, inboundChannel);

          // Setting up server acceptor subscription
          return resources
              .subscription(inboundChannel, options, null, this::onImageAvailable, null)
              .doOnSuccess(subscription -> this.subscription = subscription)
              .thenReturn(this)
              .doOnSuccess(handler -> logger.debug("Started {} on: {}", this, inboundChannel))
              .doOnError(
                  ex -> {
                    logger.error("Failed to start {} on: {}", this, inboundChannel);
                    dispose();
                  });
        });
  }

  /**
   * Setting up new {@link Connection} identified by {@link Image#sessionId()}. Specifically creates
   * message publication (aeron {@link io.aeron.Publication} underneath) with control-endpoint,
   * control-mode and given sessionId. Essentially creates server side MDC for concrete sessionId;
   * think of this as <i>server-side-individual-MDC</i>.
   *
   * @param image source image
   */
  private void onImageAvailable(Image image) {
    final int sessionId = image.sessionId();
    final DefaultAeronInbound inbound = new DefaultAeronInbound();

    // inbound->Sub(endpoint, sessionId)
    // outbound->Pub(control-endpoint{address:serverControlPort}, sessionId)->MDC(sessionId)
    final String inboundChannel = options.inboundUri().sessionId(sessionId).asString();
    final String outboundChannel = options.outboundUri().sessionId(sessionId).asString();

    logger.debug("{}: creating server connection", Integer.toHexString(sessionId));

    // setup cleanup hook to use it onwards
    MonoProcessor<Void> inboundUnavailable = MonoProcessor.create();

    resources
        .subscription(
            inboundChannel,
            options,
            inbound,
            img -> logger.debug("{}: created server inbound", Integer.toHexString(sessionId)),
            img -> {
              logger.debug("{}: server inbound became unavaliable", Integer.toHexString(sessionId));
              connections.remove(sessionId);
              inboundUnavailable.onComplete();
            })
        .flatMap(
            subscription ->
                resources
                    .publication(outboundChannel, options)
                    .map(
                        publication ->
                            new DefaultAeronConnection(
                                sessionId,
                                inbound,
                                new DefaultAeronOutbound(publication),
                                subscription,
                                publication)))
        .flatMap(connection -> connection.start(sessionId, handler, inboundUnavailable))
        .doOnSuccess(connection -> connections.put(sessionId, connection))
        .doOnSuccess(
            connection ->
                logger.debug(
                    "{}: created server connection: {}",
                    Integer.toHexString(sessionId),
                    outboundChannel))
        .subscribe(
            null,
            ex ->
                logger.warn(
                    "{}: failed to create server outbound, cause: {}",
                    Integer.toHexString(sessionId),
                    ex.toString()));
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
          logger.debug("Disposing {}", this);
          List<Mono<Void>> monos = new ArrayList<>();

          // dispose server acceptor subscription
          monos.add(
              Optional.ofNullable(subscription)
                  .map(s -> Mono.fromRunnable(s::dispose).then(s.onDispose()))
                  .orElse(Mono.empty()));

          // dispose all existing connections
          connections
              .values()
              .stream()
              .peek(Connection::dispose)
              .map(Connection::onDispose)
              .forEach(monos::add);

          return Mono.whenDelayError(monos).doFinally(s -> connections.clear());
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler0x" + Integer.toHexString(System.identityHashCode(this));
  }
}
