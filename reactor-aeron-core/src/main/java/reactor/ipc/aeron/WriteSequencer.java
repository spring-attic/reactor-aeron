package reactor.ipc.aeron;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

abstract class WriteSequencer<T> {

  private static final Logger logger = Loggers.getLogger(WriteSequencer.class);
  /**
   * Cast the supplied queue (SpscLinkedArrayQueue) to use its atomic dual-insert
   * backed by {@link BiPredicate#test)
   **/
  private final BiPredicate<MonoSink<?>, Object> pendingWriteOffer;

  private final Queue<?> pendingWrites;

  private volatile int wip;

  // a publisher is being drained
  private volatile boolean innerActive;

  private final Consumer<Object> discardedHandler;

  private final Scheduler scheduler;

  @SuppressWarnings("unchecked")
  public WriteSequencer(Scheduler scheduler, Consumer<Object> discardedHandler) {
    this.discardedHandler = discardedHandler;
    this.scheduler = scheduler;
    this.pendingWrites = Queues.unbounded().get();
    this.pendingWriteOffer = (BiPredicate<MonoSink<?>, Object>) pendingWrites;
  }

  abstract InnerSubscriber<T> getInner();

  abstract Consumer<Throwable> getErrorHandler();

  public Mono<Void> add(Publisher<?> publisher) {
    return Mono.create(
        sink -> {
          boolean success = pendingWriteOffer.test(sink, publisher);
          if (!success) {
            sink.error(new Exception("Failed to enqueue publisher"));
          }

          scheduleDrain();
        });
  }

  public boolean isEmpty() {
    return pendingWrites.isEmpty();
  }

  boolean isReady() {
    return !getInner().isCancelled;
  }

  @SuppressWarnings("unchecked")
  public void drain() {
    InnerSubscriber<T> inner = getInner();
    if (WIP.getAndIncrement(this) == 0) {

      for (; ; ) {
        if (inner.isCancelled) {
          discard();

          inner.isCancelled = false;

          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        if (innerActive) {
          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        MonoSink<?> promise;
        Object v = pendingWrites.poll();

        try {
          promise = (MonoSink<?>) v;
        } catch (Throwable e) {
          getErrorHandler().accept(e);
          return;
        }

        boolean empty = promise == null;

        if (empty) {
          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        v = pendingWrites.poll();
        Publisher<T> p = (Publisher<T>) v;

        if (p instanceof Callable) {
          @SuppressWarnings("unchecked")
          Callable<?> supplier = (Callable<?>) p;

          Object vr;

          try {
            vr = supplier.call();
          } catch (Throwable e) {
            promise.error(e);
            continue;
          }

          if (vr == null) {
            promise.success();
            continue;
          }

          innerActive = true;
          inner.setResultSink(promise);
          inner.onSubscribe(Operators.scalarSubscription(inner, (T) vr));
        } else {
          innerActive = true;
          inner.setResultSink(promise);
          p.subscribe(inner);
        }
      }
    }
  }

  void discard() {
    while (!pendingWrites.isEmpty()) {
      Object v = pendingWrites.poll();
      MonoSink<?> promise;
      try {
        promise = (MonoSink<?>) v;
      } catch (Throwable e) {
        getErrorHandler().accept(e);
        return;
      }
      v = pendingWrites.poll();
      if (logger.isDebugEnabled()) {
        logger.debug("Terminated. Dropping: {}", v);
      }

      discardedHandler.accept(v);

      promise.error(new AbortedException());
    }
  }

  void scheduleDrain() {
    scheduler.schedule(this::drain);
  }

  abstract static class InnerSubscriber<T> implements CoreSubscriber<T>, Subscription {

    final WriteSequencer<T> parent;

    volatile Subscription missedSubscription;
    volatile long missedRequested;
    volatile long missedProduced;
    volatile int wip;

    // a subscription has been cancelled
    volatile boolean isCancelled;

    /** The current outstanding request amount. */
    long requested;

    boolean unbounded;

    /** The current subscription which may null if no Subscriptions have been set. */
    Subscription actual;

    long produced;
    MonoSink<?> promise;
    long upstreamRequested;
    final int batchSize;

    InnerSubscriber(WriteSequencer<T> parent, int batchSize) {
      this.parent = parent;
      this.batchSize = batchSize;
    }

    public void setResultSink(MonoSink<?> promise) {
      this.promise = promise;
    }

    @Override
    public final void cancel() {
      if (!isCancelled) {
        isCancelled = true;

        drain();
      }
    }

    @Override
    public void onComplete() {
      long p = produced;
      parent.innerActive = false;

      if (p != 0L) {
        produced = 0L;
        produced(p);
      }

      doOnComplete();
    }

    abstract void doOnComplete();

    @Override
    public void onError(Throwable t) {
      long p = produced;
      parent.innerActive = false;

      if (p != 0L) {
        produced = 0L;
        produced(p);
      }

      doOnError(t);
    }

    abstract void doOnError(Throwable t);

    @Override
    public void onNext(T t) {
      produced++;

      doOnNext(t);

      if (upstreamRequested - produced == 0 && requested - produced > 0) {
        requestFromUpstream(actual);
      }
    }

    abstract void doOnNext(T t);

    @Override
    public void onSubscribe(Subscription s) {
      Objects.requireNonNull(s);

      if (isCancelled) {
        s.cancel();
        return;
      }

      if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        actual = s;
        upstreamRequested = 0;

        doOnSubscribe();

        long r = requested;

        if (WIP.decrementAndGet(this) != 0) {
          drainLoop();
        }

        if (r != 0L) {
          requestFromUpstream(s);
        }

        return;
      }

      MISSED_SUBSCRIPTION.set(this, s);
      drain();
    }

    abstract void doOnSubscribe();

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
            requestFromUpstream(a);
          }

          return;
        }

        Operators.addCap(MISSED_REQUESTED, this, n);

        drain();
      }
    }

    final void requestFromUpstream(Subscription s) {
      if (upstreamRequested < requested) {
        upstreamRequested += batchSize;

        s.request(batchSize);
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

        if (isCancelled) {
          if (a != null) {
            a.cancel();
            actual = null;
            upstreamRequested = 0;
          }
          if (ms != null) {
            ms.cancel();
          }

          parent.innerActive = false;
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
            actual = ms;
            upstreamRequested = 0;
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
            requestFromUpstream(requestTarget);
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

    protected void scheduleNextPublisherDrain() {
      parent.scheduleDrain();
    }

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<InnerSubscriber, Subscription> MISSED_SUBSCRIPTION =
        AtomicReferenceFieldUpdater.newUpdater(
            InnerSubscriber.class, Subscription.class, "missedSubscription");

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<InnerSubscriber> MISSED_REQUESTED =
        AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class, "missedRequested");

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<InnerSubscriber> MISSED_PRODUCED =
        AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class, "missedProduced");

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<InnerSubscriber> WIP =
        AtomicIntegerFieldUpdater.newUpdater(InnerSubscriber.class, "wip");
  }

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<WriteSequencer> WIP =
      AtomicIntegerFieldUpdater.newUpdater(WriteSequencer.class, "wip");
}
