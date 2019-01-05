package reactor.aeron.server;

import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.OnDisposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 *
 *
 * <pre>
 * Server
 * serverPort->inbound->Sub(endpoint, acceptor[onImageAvailable, onImageUnavailbe])
 * + onImageAvailable(Image)
 * sessionId->inbound->EmitterPocessor
 * serverControlPort->outbound->MDC(sessionId)->Pub(control-endpoint, sessionId)
 * </pre>
 */
final class AeronServerHandler implements FragmentHandler, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  // TODO think of more performant concurrent hashmap
  private final Map<Integer, SessionHandler> connections = new ConcurrentHashMap<>(32);

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
            th -> logger.warn("{} disposed with error: {}", this, th.toString()),
            () -> logger.debug("{} disposed", this));
  }

  public Mono<OnDisposable> start() {
    return Mono.defer(
        () -> {
          // Sub(endpoint{address:serverPort})
          String inboundChannel = options.inboundUri().asString();

          logger.debug("Starting {} on: {}", this, inboundChannel);

          return resources
              .subscription(
                  inboundChannel,
                  this, /*fragmentHandler*/
                  this::onImageAvailable, /*setup new session*/
                  this::onImageUnavailable /*remove and dispose session*/)
              .thenReturn(this)
              .doOnSuccess(handler -> logger.debug("{} started on: {}", this, inboundChannel))
              .onErrorResume(
                  ex -> {
                    logger.error("{} failed to start on: {}", this, inboundChannel);
                    dispose();
                    return onDispose().thenReturn(this);
                  });
        });
  }

  /**
   * Inner implementation of aeron's {@link FragmentHandler}. Sits on the server inbound channel and
   * serves all incoming sessions. By {@link Header#sessionId()} corresponding {@link
   * SessionHandler} is being found and message passed there.
   */
  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int sessionId = header.sessionId();
    SessionHandler sessionHandler = connections.get(sessionId);

    if (sessionHandler == null) {
      logger.warn(
          "{}: received message but sessionHandler not found (total connections: {})",
          Integer.toHexString(sessionId),
          connections.size());
      return;
    }

    sessionHandler.inbound.onFragment(buffer, offset, length, header);
  }

  /**
   * Setting up new {@link SessionHandler} identified by {@link Image#sessionId()}. Specifically
   * creates message publication (aeron {@link io.aeron.Publication} underneath) with
   * control-endpoint, control-mode and given sessionId. Essentially creates server side MDC for
   * concrete sessionId; think of this as <i>server-side-individual-MDC</i>.
   *
   * @param image source image
   */
  private void onImageAvailable(Image image) {
    int sessionId = image.sessionId();
    // Pub(control-endpoint{address:serverControlPort}, sessionId)->MDC(sessionId)
    String outboundChannel = options.outboundUri().sessionId(sessionId).asString();

    // not expecting following condition be true at normal circumstances; passing it would mean
    // aeron changed contract/semantic around sessionId
    if (connections.containsKey(sessionId)) {
      logger.error(
          "{}: sessionHandler already exists: {}", Integer.toHexString(sessionId), outboundChannel);
      return;
    }

    logger.debug(
        "{}: creating sessionHandler: {}", Integer.toHexString(sessionId), outboundChannel);

    Duration connectTimeout = options.connectTimeout();
    Duration backpressureTimeout = options.backpressureTimeout();

    resources
        .publication(outboundChannel, connectTimeout, backpressureTimeout)
        .flatMap(MessagePublication::ensureConnected)
        .doOnSuccess(publication -> setupConnection(sessionId, publication))
        .subscribe(
            null,
            ex ->
                logger.warn(
                    "{}: exception occurred on creating sessionHandler: {}, cause: {}",
                    Integer.toHexString(sessionId),
                    outboundChannel,
                    ex.toString()),
            () ->
                logger.debug(
                    "{}: created sessionHandler: {}",
                    Integer.toHexString(sessionId),
                    outboundChannel));
  }

  /**
   * Removes {@link SessionHandler} (and then disposes it) by incoming {@link Image#sessionId()}.
   * See also {@link SessionHandler#dispose()}.
   *
   * @param image source image
   */
  private void onImageUnavailable(Image image) {
    int sessionId = image.sessionId();
    SessionHandler sessionHandler = connections.remove(sessionId);

    if (sessionHandler != null) {
      sessionHandler.dispose();
      logger.debug(
          "{}: removed and disposed sessionHandler: {}",
          Integer.toHexString(sessionId),
          sessionHandler);
    } else {
      logger.debug(
          "{}: attempt to remove sessionHandler but it wasn't found (total connections: {})",
          Integer.toHexString(sessionId),
          connections.size());
    }
  }

  /**
   * Creates and setting up {@link SessionHandler}.
   *
   * @param sessionId session id
   * @param publication MDC message publication aka <i>server-side-individual-MDC</i>
   */
  private void setupConnection(int sessionId, MessagePublication publication) {
    SessionHandler connection = new SessionHandler(sessionId, publication);

    connections.put(sessionId, connection);

    // register cleanup hook
    connection
        .onDispose()
        .doFinally(s -> connections.remove(sessionId))
        .subscribe(
            null,
            th -> {
              // no-op
            });

    try {
      handler
          .apply(connection) // apply handler function
          .subscribe(connection.disposeSubscriber());
    } catch (Exception ex) {
      logger.error("{}: unexpected exception occurred on handler.apply(), cause: ", sessionId, ex);
      connection.dispose();
      throw Exceptions.propagate(ex);
    }
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
          logger.debug("{} is disposing", this);
          return Mono.whenDelayError(
                  connections
                      .values()
                      .stream()
                      .peek(SessionHandler::dispose)
                      .map(SessionHandler::onDispose)
                      .collect(Collectors.toList()))
              .doFinally(
                  s -> {
                    logger.debug("{} disposed", this);
                    connections.clear();
                  });
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler0x" + Integer.toHexString(System.identityHashCode(this));
  }

  /** Server side wrapper around {@link AeronInbound} and {@link AeronOutbound}. */
  private static class SessionHandler implements Connection {

    private final Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    private final int sessionId;

    private final DefaultAeronInbound inbound;
    private final DefaultAeronOutbound outbound;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    /**
     * Construcotr.
     *
     * @param sessionId session id
     * @param publication MDC message publication aka <i>server-side-individual-MDC</i>
     */
    private SessionHandler(int sessionId, MessagePublication publication) {
      this.sessionId = sessionId;
      this.inbound = new DefaultAeronInbound();
      this.outbound = new DefaultAeronOutbound(publication);

      this.dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(
              null,
              th ->
                  logger.warn(
                      "{}: sessionHandler disposed with error: {}",
                      Integer.toHexString(sessionId),
                      th.toString()));
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
      return ""; // TODO implement
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
            logger.debug("{}: is disposing {}", Integer.toHexString(sessionId), this);
            return Mono.whenDelayError(
                    Mono.fromRunnable(inbound::dispose).then(inbound.onDispose()),
                    Mono.fromRunnable(outbound::dispose).then(outbound.onDispose()))
                .doFinally(
                    s -> logger.debug("{}: disposed {}", Integer.toHexString(sessionId), this));
          });
    }
  }
}
