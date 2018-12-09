package reactor.aeron;

import java.nio.ByteBuffer;
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
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

final class AeronWriteSequencer {

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<AeronWriteSequencer> WIP =
      AtomicIntegerFieldUpdater.newUpdater(AeronWriteSequencer.class, "wip");

  private static final Logger logger = Loggers.getLogger(AeronWriteSequencer.class);

  private final AeronEventLoop eventLoop;

  private final PublisherSender inner;
  private final int prefetch = 32;

  private final Consumer<Throwable> errorHandler;
  private final Consumer<Object> discardedHandler;

  // Cast the supplied queue (SpscLinkedArrayQueue) to use its atomic dual-insert backed by {@link
  // BiPredicate#test)
  private final BiPredicate<MonoSink<?>, Object> pendingWriteOffer;
  private final Queue<?> pendingWrites;

  private volatile boolean innerActive;
  // todo help wanted, do we need to implement Disposable in AeronWriteSequencer?
  private volatile boolean removed;

  private volatile int wip;

  AeronWriteSequencer(long sessionId, MessagePublication publication, AeronEventLoop eventLoop) {
    this.eventLoop = eventLoop;
    this.discardedHandler =
        o -> {
          // no-op
        };
    this.pendingWrites = Queues.unbounded().get();
    //noinspection unchecked
    this.pendingWriteOffer = (BiPredicate<MonoSink<?>, Object>) pendingWrites;
    this.errorHandler = throwable -> logger.error("Unexpected exception", throwable);
    this.inner = new PublisherSender(this, publication, sessionId);
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencer} instance.
   *
   * @param publisher data publisher
   * @return mono handle
   */
  public Mono<Void> write(Publisher<?> publisher) {
    return Mono.defer(
        () ->
            eventLoop.execute(
                sink -> {
                  boolean result = pendingWriteOffer.test(sink, publisher);
                  if (!result) {
                    sink.error(new Exception("Failed to enqueue publisher"));
                  }
                  drain();
                }));
  }

  void drain() {
    if (WIP.getAndIncrement(this) == 0) {

      for (; ; ) {

        if (removed) { // todo maybe cancel of message publication or inner.isCancelled?
          discard();
          return;
        }

        //  if (inner.isCancelled) {
        //    discard();
        //    inner.isCancelled = false;
        //    if (WIP.decrementAndGet(this) == 0) {
        //      break;
        //    }
        //    continue;
        //  }

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
          errorHandler.accept(e);
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
        //noinspection unchecked
        Publisher<ByteBuffer> p = (Publisher<ByteBuffer>) v;

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
          inner.promise = promise;
          inner.onSubscribe(Operators.scalarSubscription(inner, (ByteBuffer) vr));
        } else {
          innerActive = true;
          inner.promise = promise;
          //noinspection ConstantConditions
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
        errorHandler.accept(e);
        return;
      }
      v = pendingWrites.poll();
      if (logger.isDebugEnabled()) {
        logger.debug("Terminated. Dropping: {}", v);
      }

      discardedHandler.accept(v);

      promise.error(new AbortedException("Something has been discarded"));
    }
  }

  static class PublisherSender implements CoreSubscriber<ByteBuffer>, Subscription {

    static final AtomicReferenceFieldUpdater<PublisherSender, Subscription> MISSED_SUBSCRIPTION =
        AtomicReferenceFieldUpdater.newUpdater(
            PublisherSender.class, Subscription.class, "missedSubscription");

    static final AtomicLongFieldUpdater<PublisherSender> MISSED_REQUESTED =
        AtomicLongFieldUpdater.newUpdater(PublisherSender.class, "missedRequested");

    static final AtomicLongFieldUpdater<PublisherSender> MISSED_PRODUCED =
        AtomicLongFieldUpdater.newUpdater(PublisherSender.class, "missedProduced");

    static final AtomicIntegerFieldUpdater<PublisherSender> WIP =
        AtomicIntegerFieldUpdater.newUpdater(PublisherSender.class, "wip");

    private final AeronWriteSequencer parent;

    private final long sessionId;

    private final MessagePublication publication;
    private volatile Subscription missedSubscription;
    private volatile long missedRequested;
    private volatile long missedProduced;
    private volatile int wip;

    private boolean inactive;

    /** The current outstanding request amount. */
    private long requested;

    private boolean unbounded;
    /** The current subscription which may null if no Subscriptions have been set. */
    private Subscription actual;

    private long produced;
    private MonoSink<?> promise;

    PublisherSender(AeronWriteSequencer parent, MessagePublication publication, long sessionId) {
      this.parent = parent;
      this.sessionId = sessionId;
      this.publication = publication;
    }

    @Override
    public final void cancel() {
      // full stop
      if (!inactive) {
        inactive = true;

        drain();
      }
    }

    @Override
    public void onComplete() {
      long p = produced;
      parent.innerActive = false;

      produced = 0L;
      produced(p);

      promise.success();
      parent.drain();
    }

    @Override
    public void onError(Throwable t) {
      long p = produced;
      parent.innerActive = false;

      produced = 0L;
      produced(p);

      promise.error(t);
      parent.drain();
    }

    @Override
    public void onNext(ByteBuffer t) {
      AeronEventLoop eventLoop = parent.eventLoop;
      if (eventLoop.inEventLoop()) {
        onNextInternal(t, null);
      } else {
        eventLoop
            .execute(sink -> onNextInternal(t, sink))
            .subscribe(null, this::disposeCurrentDataStream);
      }
    }

    private void onNextInternal(ByteBuffer byteBuffer, MonoSink<Void> sink) {
      // TODO : think of lastWrite field
      // TODO : think what to do with passed sink
      produced++;

      publication
          .enqueue(MessageType.NEXT, byteBuffer, sessionId)
          .doOnSuccess(avoid -> request(1L))
          .subscribe(null, this::disposeCurrentDataStream);
    }

    private void disposeCurrentDataStream(Throwable th) {
      cancel();
      promise.error(new Exception("Failed to publish signal into session: " + sessionId, th));
      parent.drain();
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (inactive) {
        s.cancel();
        return;
      }

      Objects.requireNonNull(s);

      if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        actual = s;
        request(parent.prefetch);

        long r = requested;

        if (WIP.decrementAndGet(this) != 0) {
          drainLoop();
        }

        if (r != 0L) {
          s.request(r);
        }

        return;
      }

      MISSED_SUBSCRIPTION.set(this, s);
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

          if (ms == Operators.cancelledSubscription()) {
            parent.innerActive = false;
            Subscription a = actual;
            if (a != null) {
              a.cancel();
              actual = null;
            }
          }
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
  }
}
