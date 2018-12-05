package reactor.aeron;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;

public final class InnerPoller
    implements org.reactivestreams.Subscription, OnDisposable, AutoCloseable {

  private static final AtomicLongFieldUpdater<InnerPoller> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(InnerPoller.class, "requested");

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
  public InnerPoller(
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

  @Override
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Can only close aeron subscription from within event loop");
    }

    try {
      subscription.close();
    } finally {
      onDispose.onComplete();
    }
  }

  @Override
  public void dispose() {
    eventLoop
        .dispose(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  @Override
  public boolean isDisposed() {
    return subscription.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }
}
