package reactor.aeron;

import io.aeron.Image;
import io.aeron.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class MessageSubscription implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessageSubscription.class);

  private final AeronEventLoop eventLoop;
  private final Subscription subscription; // aeron subscription

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param subscription aeron subscription
   * @param eventLoop event loop where this {@code MessageSubscription} is assigned
   */
  public MessageSubscription(Subscription subscription, AeronEventLoop eventLoop) {
    this.subscription = subscription;
    this.eventLoop = eventLoop;
  }

  /**
   * Closes aeron {@link Subscription}. Can only be called from within {@link AeronEventLoop} worker
   * thred.
   *
   * <p><b>NOTE:</b> this method is not for public client (despite it was declared with {@code
   * public} signifier).
   */
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Can only close aeron subscription from within event loop");
    }
    try {
      subscription.close();
      logger.debug("Disposed {}", this);
    } catch (Exception ex) {
      logger.warn("{} failed on aeron.Subscription close(): {}", this, ex.toString());
      throw Exceptions.propagate(ex);
    } finally {
      onDispose.onComplete();
    }
  }

  @Override
  public void dispose() {
    eventLoop
        .disposeSubscription(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  /**
   * Delegates to {@link Subscription#isClosed()}.
   *
   * @return {@code true} if aeron {@code Subscription} is closed, {@code false} otherwise
   */
  @Override
  public boolean isDisposed() {
    return subscription.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public String toString() {
    return "MessageSubscription{sub=" + subscription.channel() + "}";
  }
}
