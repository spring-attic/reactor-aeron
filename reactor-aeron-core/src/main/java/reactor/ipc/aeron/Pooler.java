package reactor.ipc.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Pooler. */
public class Pooler implements Runnable {

  private static final Logger logger = Loggers.getLogger(Pooler.class);

  private final String name;

  private final ExecutorService executor;

  private volatile boolean isRunning;

  private volatile InnerPooler[] poolers = new InnerPooler[0];

  public Pooler(String name) {
    this.name = name;
    this.executor = createExecutor(name);
  }

  private ExecutorService createExecutor(String name) {
    return Executors.newSingleThreadExecutor(
        r -> {
          Thread thread = new Thread(r, name + "-[pooler]");
          thread.setDaemon(true);
          return thread;
        });
  }

  // FIXME: Thread-safety

  /** Init method. */
  public void initialise() {
    if (!isRunning) {
      isRunning = true;
      executor.submit(this);
    }
  }

  public void addControlSubscription(
      Subscription subscription, ControlMessageSubscriber subscriber) {
    InnerPooler pooler = new InnerPooler(subscription, subscriber);
    addPooler(pooler);
  }

  public void addDataSubscription(Subscription subscription, DataMessageSubscriber subscriber) {
    InnerPooler pooler = new InnerPooler(subscription, subscriber);
    addPooler(pooler);
  }

  private synchronized void addPooler(InnerPooler pooler) {
    this.poolers = ArrayUtil.add(poolers, pooler);
  }

  /**
   * Shutdown method.
   *
   * @return shutdown reference
   */
  public Mono<Void> shutdown() {
    return Mono.create(
        sink -> {
          isRunning = false;
          AtomicBoolean shouldRetry = new AtomicBoolean(true);
          sink.onCancel(() -> shouldRetry.set(false));
          executor.shutdown();
          try {
            while (shouldRetry.get()) {
              boolean isTerminated = executor.awaitTermination(1, TimeUnit.SECONDS);
              if (isTerminated) {
                break;
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          sink.success();

          logger.debug("Terminated");
        });
  }

  @Override
  public void run() {
    logger.debug("[{}] Started", name);

    BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
    while (isRunning) {
      InnerPooler[] ss = poolers;
      int numOfReceived = 0;
      for (InnerPooler data : ss) {
        numOfReceived = data.poll();
      }
      idleStrategy.idle(numOfReceived);
    }

    logger.debug("[{}] Terminated", name);
  }

  /**
   * Removes subscription.
   *
   * @param subscription subscription
   */
  public synchronized void removeSubscription(Subscription subscription) {
    InnerPooler[] ss = poolers;
    for (int i = 0; i < ss.length; i++) {
      InnerPooler pooler = ss[i];
      if (pooler.subscription == subscription) {
        this.poolers = ArrayUtil.remove(poolers, i);
        break;
      }
    }
  }

  static class InnerPooler implements org.reactivestreams.Subscription {

    final Subscription subscription;

    final FragmentHandler handler;

    volatile long requested = 0;

    private static final AtomicLongFieldUpdater<InnerPooler> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(InnerPooler.class, "requested");

    InnerPooler(Subscription subscription, ControlMessageSubscriber subscriber) {
      this(subscription, subscriber, new ControlFragmentHandler(subscriber));
    }

    InnerPooler(Subscription subscription, DataMessageSubscriber subscriber) {
      this(subscription, subscriber, new DataFragmentHandler(subscriber));
    }

    private InnerPooler(
        Subscription subscription, PoolerSubscriber subscriber, FragmentHandler handler) {
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
