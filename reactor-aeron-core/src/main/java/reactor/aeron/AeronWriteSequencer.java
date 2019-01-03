package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;

final class AeronWriteSequencer {

  private static final int PREFETCH = 32;

  private final MessagePublication publication;

  /**
   * Constructor for templating {@link AeronWriteSequencer} objects.
   *
   * @param publication message publication
   */
  AeronWriteSequencer(MessagePublication publication) {
    this.publication = Objects.requireNonNull(publication, "message publication must be present");
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencer} instance.
   *
   * @param publisher data publisher
   * @return mono handle
   */
  public Mono<Void> write(Publisher<? extends ByteBuffer> publisher) {
    Objects.requireNonNull(publisher, "publisher must be not null");
    return Mono.defer(
        () -> {
          PublisherSender inner = new PublisherSender(publication);
          publisher.subscribe(inner);
          inner.request(PREFETCH);
          return inner
              .newPromise
              .takeUntilOther(publication.onDispose())
              .doFinally(s -> inner.cancel());
        });
  }

  private static class PublisherSender extends MultiSubscriptionSubscriber<ByteBuffer> {

    private final MessagePublication publication;

    private long produced;

    private volatile boolean completed;

    private final MonoProcessor<Void> newPromise = MonoProcessor.create();

    PublisherSender(MessagePublication publication) {
      this.publication = publication;
    }

    @Override
    public void onComplete() {
      long p = produced;

      produced = 0L;
      produced(p);

      completed = true;
    }

    @Override
    public void onError(Throwable t) {
      long p = produced;

      produced = 0L;
      produced(p);

      newPromise.onError(t);
    }

    @Override
    public void onNext(ByteBuffer t) {
      produced++;

      publication
          .enqueue(t)
          .doOnSuccess(
              avoid -> {
                if (completed) {
                  newPromise.onComplete();
                  return;
                }
                request(1L);
              })
          .subscribe(
              null,
              th -> {
                cancel();
                newPromise.onError(new Exception("Failed to publish signal", th));
              });
    }
  }

  /**
   * It's similar to {@link Operators.MultiSubscriptionSubscriber}.
   *
   * <p>A subscription implementation that arbitrates request amounts between subsequent
   * Subscriptions, including the duration until the first Subscription is set.
   *
   * <p>The class is thread safe but switching Subscriptions should happen only when the source
   * associated with the current Subscription has finished emitting values. Otherwise, two sources
   * may emit for one request.
   *
   * <p>You should call {@link #produced(long)} after each element has been delivered to properly
   * account the outstanding request amount in case a Subscription switch happens.
   *
   * @param <I> the input value type
   */
  private abstract static class MultiSubscriptionSubscriber<I>
      implements CoreSubscriber<I>, Subscription {

    private static final AtomicReferenceFieldUpdater<MultiSubscriptionSubscriber, Subscription>
        MISSED_SUBSCRIPTION =
            AtomicReferenceFieldUpdater.newUpdater(
                MultiSubscriptionSubscriber.class, Subscription.class, "missedSubscription");
    private static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_REQUESTED =
        AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedRequested");
    private static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_PRODUCED =
        AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedProduced");
    private static final AtomicIntegerFieldUpdater<MultiSubscriptionSubscriber> WIP =
        AtomicIntegerFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "wip");

    private boolean unbounded;
    /** The current subscription which may null if no Subscriptions have been set. */
    private Subscription actual;
    /** The current outstanding request amount. */
    private long requested;

    private volatile Subscription missedSubscription;
    private volatile long missedRequested;
    private volatile long missedProduced;
    private volatile int wip;
    private volatile boolean inactive;

    @Override
    public void cancel() {
      if (!inactive) {
        inactive = true;

        drain();
      }
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (inactive) {
        s.cancel();
        return;
      }

      Objects.requireNonNull(s);

      if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        Subscription a = actual;

        if (a != null && shouldCancelCurrent()) {
          a.cancel();
        }

        actual = s;

        long r = requested;

        if (WIP.decrementAndGet(this) != 0) {
          drainLoop();
        }

        if (r != 0L) {
          s.request(r);
        }

        return;
      }

      Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
      if (a != null && shouldCancelCurrent()) {
        a.cancel();
      }
      drain();
    }

    @Override
    public final void request(long n) {
      if (Operators.validate(n)) {
        if (unbounded) {
          return;
        }
        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
          long r = requested;

          if (r != Long.MAX_VALUE) {
            r = Operators.addCap(r, n);
            requested = r;
            if (r == Long.MAX_VALUE) {
              unbounded = true;
            }
          }
          Subscription a = actual;

          if (WIP.decrementAndGet(this) != 0) {
            drainLoop();
          }

          if (a != null) {
            a.request(n);
          }

          return;
        }

        Operators.addCap(MISSED_REQUESTED, this, n);

        drain();
      }
    }

    final void drain() {
      if (WIP.getAndIncrement(this) != 0) {
        return;
      }
      drainLoop();
    }

    final void drainLoop() {
      int missed = 1;

      long requestAmount = 0L;
      Subscription requestTarget = null;

      for (; ; ) {

        Subscription ms = missedSubscription;

        if (ms != null) {
          ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
        }

        long mr = missedRequested;
        if (mr != 0L) {
          mr = MISSED_REQUESTED.getAndSet(this, 0L);
        }

        long mp = missedProduced;
        if (mp != 0L) {
          mp = MISSED_PRODUCED.getAndSet(this, 0L);
        }

        Subscription a = actual;

        if (inactive) {
          if (a != null) {
            a.cancel();
            actual = null;
          }
          if (ms != null) {
            ms.cancel();
          }
        } else {
          long r = requested;
          if (r != Long.MAX_VALUE) {
            long u = Operators.addCap(r, mr);

            if (u != Long.MAX_VALUE) {
              long v = u - mp;
              if (v < 0L) {
                Operators.reportMoreProduced();
                v = 0;
              }
              r = v;
            } else {
              r = u;
            }
            requested = r;
          }

          if (ms != null) {
            if (a != null && shouldCancelCurrent()) {
              a.cancel();
            }
            actual = ms;
            if (r != 0L) {
              requestAmount = Operators.addCap(requestAmount, r);
              requestTarget = ms;
            }
          } else if (mr != 0L && a != null) {
            requestAmount = Operators.addCap(requestAmount, mr);
            requestTarget = a;
          }
        }

        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) {
          if (requestAmount != 0L) {
            requestTarget.request(requestAmount);
          }
          return;
        }
      }
    }

    final void produced(long n) {
      if (unbounded) {
        return;
      }
      if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        long r = requested;

        if (r != Long.MAX_VALUE) {
          long u = r - n;
          if (u < 0L) {
            Operators.reportMoreProduced();
            u = 0;
          }
          requested = u;
        } else {
          unbounded = true;
        }

        if (WIP.decrementAndGet(this) == 0) {
          return;
        }

        drainLoop();

        return;
      }

      Operators.addCap(MISSED_PRODUCED, this, n);

      drain();
    }

    /**
     * When setting a new subscription via {@link #onSubscribe}, should the previous subscription be
     * cancelled?.
     *
     * @return true if cancellation is needed
     */
    boolean shouldCancelCurrent() {
      return false;
    }
  }
}
