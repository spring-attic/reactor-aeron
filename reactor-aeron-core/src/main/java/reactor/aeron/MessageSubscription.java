package reactor.aeron;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;

public final class MessageSubscription implements org.reactivestreams.Subscription, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessageSubscription.class);

  private static final AtomicLongFieldUpdater<MessageSubscription> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(MessageSubscription.class, "requested");

  private final AeronEventLoop eventLoop;
  private final Subscription subscription;
  private final FragmentHandler fragmentHandler;

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  @SuppressWarnings("FieldCanBeLocal")
  private volatile long requested = 0;

  /**
   * Constructor for message subscriptino.
   *
   * @param eventLoop event loop where this message subscription is assigned
   * @param subscription aeron subscription
   * @param fragmentHandler aeron fragment handler
   */
  public MessageSubscription(
      AeronEventLoop eventLoop, Subscription subscription, FragmentHandler fragmentHandler) {
    this.eventLoop = eventLoop;
    this.subscription = subscription;
    this.fragmentHandler = fragmentHandler;
  }

  /**
   * Subscrptions poll method.
   *
   * @return the number of fragments received
   */
  public int poll() {
    int r = (int) Math.min(requested, 8);
    int numOfPolled = 0;
    if (r > 0) {
      numOfPolled = subscription.poll(fragmentHandler, r);
      System.out.println("OK, let's go POLL");
      if (numOfPolled > 0) {
        Operators.produced(REQUESTED, this, numOfPolled);
      }
    }
    return numOfPolled;
  }

  @Override
  public void request(long n) {
    Operators.addCap(REQUESTED, this, n);
  }

  @Override
  public void cancel() {}

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
      logger.debug("aeron.Subscription closed: {}", this);
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
