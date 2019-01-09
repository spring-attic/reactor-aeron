package reactor.aeron;

import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron <i>connection</i>. Bound to certain {@code sessionId}. Implements {@link
 * OnDisposable} for convenient resource cleanup.
 */
public final class DefaultAeronConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(DefaultAeronConnection.class);

  private final int sessionId;

  private final DefaultAeronInbound inbound;
  private final DefaultAeronOutbound outbound;
  private final MessagePublication publication;
  private final MessageSubscription subscription;

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param sessionId session id
   * @param inbound inbound
   * @param outbound outbound
   * @param subscription subscription
   * @param publication publication
   */
  public DefaultAeronConnection(
      int sessionId,
      DefaultAeronInbound inbound,
      DefaultAeronOutbound outbound,
      MessageSubscription subscription,
      MessagePublication publication) {

    this.sessionId = sessionId;
    this.inbound = inbound;
    this.outbound = outbound;
    this.subscription = subscription;
    this.publication = publication;

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  /**
   * Setting up this connection by applying user provided handler and connection shutdown hook.
   *
   * @param sessionId session id
   * @param handler handler with application level code
   * @param disposeHook connection shutdown hook
   */
  public Mono<Connection> start(
      int sessionId,
      Function<? super Connection, ? extends Publisher<Void>> handler,
      MonoProcessor<Void> disposeHook) {

    return Mono.fromRunnable(() -> start0(sessionId, disposeHook, handler)).thenReturn(this);
  }

  private void start0(
      int sessionId,
      MonoProcessor<Void> disposeHook,
      Function<? super Connection, ? extends Publisher<Void>> handler) {

    // listen shutdown
    disposeHook
        .then(Mono.fromRunnable(this::dispose).then(onDispose()))
        .log("disposeHook")
        .doFinally(s -> logger.debug("{}: connection disposed", Integer.toHexString(sessionId)))
        .subscribe(
            null,
            th -> {
              // no-op
            });

    if (handler == null) {
      logger.warn("{}: handler function is not specified", Integer.toHexString(sessionId));
      return;
    }

    try {
      if (!isDisposed()) {
        handler.apply(this).subscribe(disposeSubscriber());
      }
    } catch (Exception ex) {
      logger.error(
          "{}: unexpected exception occurred on handler.apply(), cause: ",
          Integer.toHexString(sessionId),
          ex);
      dispose();
      throw Exceptions.propagate(ex);
    }
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
              Mono.fromRunnable(inbound::dispose),
              Mono.fromRunnable(subscription::dispose).then(subscription.onDispose()),
              Mono.fromRunnable(publication::dispose).then(publication.onDispose()));
        });
  }

  @Override
  public String toString() {
    return "DefaultAeronConnection0x" + Integer.toHexString(sessionId);
  }
}
