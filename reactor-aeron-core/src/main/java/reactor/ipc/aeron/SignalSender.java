package reactor.ipc.aeron;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;

class SignalSender implements CoreSubscriber<ByteBuffer>, Subscription {

  private static final AtomicReferenceFieldUpdater<SignalSender, Subscription> MISSED_SUBSCRIPTION =
      AtomicReferenceFieldUpdater.newUpdater(
          SignalSender.class, Subscription.class, "missedSubscription");

  private static final AtomicLongFieldUpdater<SignalSender> MISSED_REQUESTED =
      AtomicLongFieldUpdater.newUpdater(SignalSender.class, "missedRequested");

  private static final AtomicLongFieldUpdater<SignalSender> MISSED_PRODUCED =
      AtomicLongFieldUpdater.newUpdater(SignalSender.class, "missedProduced");

  private static final AtomicIntegerFieldUpdater<SignalSender> WIP =
      AtomicIntegerFieldUpdater.newUpdater(SignalSender.class, "wip");

  private final int batchSize;
  private final AeronWriteSequencer sequencer;

  private final long sessionId;

  private final MessagePublication publication;
  private volatile Subscription missedSubscription;
  private volatile long missedRequested;
  private volatile long missedProduced;
  private volatile int wip;

  // a subscription has been cancelled
  private volatile boolean isCancelled;

  // a publisher is being drained
  private volatile boolean isActive;

  /** The current outstanding request amount. */
  private long requested;

  private boolean unbounded;
  /** The current subscription which may null if no Subscriptions have been set. */
  private Subscription actual;

  private long produced;
  private MonoSink<?> promise;
  private long upstreamRequested;

  SignalSender(AeronWriteSequencer sequencer, MessagePublication publication, long sessionId) {
    this.batchSize = 16;
    this.sequencer = sequencer;
    this.sessionId = sessionId;
    this.publication = publication;
  }

  void doOnSubscribe() {
    request(Long.MAX_VALUE);
  }

  void doOnNext(ByteBuffer byteBuffer) {
    Exception cause = null;
    long result = 0;
    try {
      result = publication.publish(MessageType.NEXT, byteBuffer, sessionId);
      if (result > 0) {
        return;
      }
    } catch (Exception ex) {
      cause = ex;
    }

    cancel();

    promise.error(
        new Exception(
            "Failed to publish signal into session with Id: " + sessionId + ", result: " + result,
            cause));

    sequencer.scheduleDrain();
  }

  void doOnError(Throwable t) {
    promise.error(t);

    sequencer.scheduleDrain();
  }

  void doOnComplete() {
    promise.success();

    sequencer.scheduleDrain();
  }

  void setResultSink(MonoSink<?> promise) {
    this.promise = promise;
  }

  boolean isCancelled() {
    return isCancelled;
  }

  void setCancelled(boolean isCancelled) {
    this.isCancelled = isCancelled;
  }

  boolean isActive() {
    return isActive;
  }

  void setActive(boolean active) {
    isActive = active;
  }

  @Override
  public final void cancel() {
    if (!isCancelled()) {
      setCancelled(true);

      drain();
    }
  }

  @Override
  public void onComplete() {
    long p = produced;
    setActive(false);

    if (p != 0L) {
      produced = 0L;
      produced(p);
    }

    doOnComplete();
  }

  @Override
  public void onError(Throwable t) {
    long p = produced;
    setActive(false);

    if (p != 0L) {
      produced = 0L;
      produced(p);
    }

    doOnError(t);
  }

  @Override
  public void onNext(ByteBuffer t) {
    produced++;

    doOnNext(t);

    if (upstreamRequested - produced == 0 && requested - produced > 0) {
      requestFromUpstream(actual);
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    Objects.requireNonNull(s);

    if (isCancelled()) {
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

      if (isCancelled()) {
        if (a != null) {
          a.cancel();
          actual = null;
          upstreamRequested = 0;
        }
        if (ms != null) {
          ms.cancel();
        }

        setActive(false);
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
}
