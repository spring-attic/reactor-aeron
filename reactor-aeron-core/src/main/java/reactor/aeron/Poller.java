package reactor.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Operators;

public class Poller implements Runnable {

  private final Supplier<Boolean> runningCondition;

  private volatile InnerPoller[] innerPollers = new InnerPoller[0];

  public Poller(Supplier<Boolean> runningCondition) {
    this.runningCondition = runningCondition;
  }

  public void addControlSubscription(
      Subscription subscription, ControlMessageSubscriber subscriber) {
    addPoller(new InnerPoller(subscription, subscriber));
  }

  public void addDataSubscription(Subscription subscription, DataMessageSubscriber subscriber) {
    addPoller(new InnerPoller(subscription, subscriber));
  }

  private synchronized void addPoller(InnerPoller poller) {
    this.innerPollers = ArrayUtil.add(innerPollers, poller);
  }

  @Override
  public void run() {
    BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
    while (runningCondition.get()) {
      InnerPoller[] pollers = innerPollers;
      int numOfReceived = 0;
      for (InnerPoller poller : pollers) {
        numOfReceived += poller.poll();
      }
      idleStrategy.idle(numOfReceived);
    }
  }

  /**
   * Removes subscription.
   *
   * @param subscription subscription
   */
  public synchronized void removeSubscription(Subscription subscription) {
    InnerPoller[] innerPollers = this.innerPollers;
    for (int i = 0; i < innerPollers.length; i++) {
      InnerPoller poller = innerPollers[i];
      if (poller.subscription == subscription) {
        this.innerPollers = ArrayUtil.remove(this.innerPollers, i);
        break;
      }
    }
  }

  static class InnerPoller implements org.reactivestreams.Subscription {

    final Subscription subscription;

    final FragmentHandler handler;

    volatile long requested = 0;

    private static final AtomicLongFieldUpdater<InnerPoller> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(InnerPoller.class, "requested");

    InnerPoller(Subscription subscription, ControlMessageSubscriber subscriber) {
      this(subscription, subscriber, new ControlFragmentHandler(subscriber));
    }

    InnerPoller(Subscription subscription, DataMessageSubscriber subscriber) {
      this(subscription, subscriber, new DataFragmentHandler(subscriber));
    }

    private InnerPoller(
        Subscription subscription, PollerSubscriber subscriber, FragmentHandler handler) {
      this.subscription = subscription;
      this.handler = new FragmentAssembler(handler);

      subscriber.onSubscribe(this);
    }

    int poll() {
      int r = (int) Math.min(requested, 8);
      int numOfPolled = 0;
      if (r > 0) {
        numOfPolled = subscription.poll(handler, r);
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
  }
}
