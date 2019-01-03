package reactor.aeron.server;

import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DataFragmentHandler;
import reactor.aeron.DataMessageProcessor;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

final class AeronServerHandler implements FragmentHandler, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private final AeronServerOptions options;
  private final AeronResources resources;

  // TODO somehow need to conjunct client given handler-function with a point where
  // Connection is created
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  // TODO think of more performant concurrent hashmap
  private final Map<Integer, SessionHandler> connections = new ConcurrentHashMap<>(32);

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronServerOptions options) {
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
          // TODO why need following two objects?
          DataMessageProcessor messageProcessor = new DataMessageProcessor();
          DataFragmentHandler fragmentHandler = new DataFragmentHandler(messageProcessor);

          String inboundChannel = options.inboundUri().asString();

          logger.debug("Starting {} on: {}", this, inboundChannel);

          return resources
              .subscription(
                  inboundChannel,
                  fragmentHandler,
                  this::onImageAvailable /*accept new session*/,
                  this::onImageUnavailable /*remove session*/)
              .thenReturn(this)
              .doOnSuccess(h -> logger.debug("{} started on: {}", this, inboundChannel));
        });
  }

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
          "{}: attempt to remove sessionHandler but it's not found (total connections: {})",
          Integer.toHexString(sessionId),
          connections.size());
    }
  }

  private void onImageAvailable(Image image) {
    int sessionId = image.sessionId();
    String outboundChannel = options.outboundUri().sessionId(sessionId).asString();

    logger.debug(
        "{}: creating sessionHandler: {}", Integer.toHexString(sessionId), outboundChannel);

    Duration connectTimeout = options.connectTimeout();
    Duration backpressureTimeout = options.backpressureTimeout();

    resources
        .publication(outboundChannel, connectTimeout, backpressureTimeout)
        .flatMap(MessagePublication::ensureConnected)
        .doOnSuccess(p -> connections.put(sessionId, new SessionHandler(sessionId, p)))
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

    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
    buffer.getBytes(offset, dstBuffer, length);
    dstBuffer.flip();

    // TODO what next
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
          return Mono.whenDelayError(
                  connections
                      .values()
                      .stream()
                      .peek(SessionHandler::dispose)
                      .map(SessionHandler::onDispose)
                      .collect(Collectors.toList()))
              .doFinally(s -> connections.clear());
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler0x" + Integer.toHexString(System.identityHashCode(this));
  }

  private static class SessionHandler implements Connection {

    private final Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    private final int sessionId;

    private final DefaultAeronOutbound outbound;
    private final DefaultAeronInbound inbound;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private SessionHandler(int sessionId, MessagePublication publication) {
      this.sessionId = sessionId;
      this.outbound = new DefaultAeronOutbound(publication);
      this.inbound = new DefaultAeronInbound();

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
            logger.debug("{}: closing {}", Integer.toHexString(sessionId), this);

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
                    s -> logger.debug("{}: closed {}", Integer.toHexString(sessionId), this));
          });
    }
  }
}
