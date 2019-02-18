package reactor.aeron.demo;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.HdrHistogram.Recorder;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.console.ContinueBarrier;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;

public class OutboundModelBenchmarkRunner {

  static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    System.out.println(
        "Using worker idle strategy "
            + Configurations.idleStrategy().getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");

    SharedState sharedState = new SharedState();

    sharedState.start();

    ContinueBarrier barrier = new ContinueBarrier("Execute again?");
    do {
      System.out.println("Writing flux " + Configurations.FLUX_REPEAT + " times");
      writeFluxWithFlatMap(sharedState);
      System.out.println("Histogram of fluxWriter latencies in microseconds.");
    } while (barrier.await());

    sharedState.stop();
  }

  private static void writeFluxWithFlatMap(SharedState sharedState) {
    HISTOGRAM.reset();

    Disposable reporter = startReport();

    int fluxThreads = Configurations.FLUX_THREADS;

    List<Thread> fluxWriters =
        IntStream.range(0, fluxThreads)
            .mapToObj(
                i -> {
                  Thread thread = new Thread(new PerThreadState(sharedState));
                  thread.setName("fluxWriter-" + i);
                  thread.start();
                  System.out.println("Started publisher writer: " + thread.getName());
                  return thread;
                })
            .collect(Collectors.toList());

    for (Thread fluxWriter : fluxWriters) {
      try {
        fluxWriter.join();
      } catch (Exception ex) {
        // no-op
      }

      reporter.dispose();
    }
  }

  private static class SharedState {

    final AtomicBoolean running = new AtomicBoolean(true);
    final IdleStrategy idleStrategy = Configurations.idleStrategy();

    final CopyOnWriteArrayList<DataStreamSubscriber> subscribers = new CopyOnWriteArrayList<>();

    Thread workerThread;

    private void start() {
      workerThread = new Thread(new SharedWorker(this));
      workerThread.setName("worker");
      workerThread.start();
      System.out.println("Started worker thread");
    }

    private void stop() throws Exception {
      running.set(false);
      workerThread.join();
    }
  }

  private static class PerThreadState implements Runnable {

    final int fluxRepeat = Configurations.FLUX_REPEAT;
    final SharedState sharedState;

    private PerThreadState(SharedState sharedState) {
      this.sharedState = sharedState;
    }

    @Override
    public void run() {
      Flux<Long> dataStream = Mono.fromCallable(System::nanoTime).repeat(fluxRepeat);

      DataStreamSubscriber dataStreamSubscriber = new DataStreamSubscriber();
      dataStream.subscribe(dataStreamSubscriber);
      sharedState.subscribers.add(dataStreamSubscriber);

      Mono.never().block();
    }
  }

  private static class DataStreamSubscriber extends BaseSubscriber<Long> {

    private static final AtomicLongFieldUpdater<DataStreamSubscriber> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(DataStreamSubscriber.class, "requested");

    volatile Object currentNext;
    volatile long requested;

    Object currentNext() {
      return currentNext;
    }

    void request() {
      if (requested < 1) {
        currentNext = null;
        Operators.addCap(REQUESTED, DataStreamSubscriber.this, 1);
        upstream().request(1);
      }
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      // subscription.request(1);
    }

    @Override
    protected void hookOnNext(Long next) {
      currentNext = next;
      Operators.produced(REQUESTED, this, 1);
    }

    @Override
    protected void hookFinally(SignalType type) {
      super.hookFinally(type);
      // remove yourself from sharedState.subscribers
    }
  }

  private static class SharedWorker implements Runnable {

    final SharedState sharedState;
    final AtomicBoolean running;
    final IdleStrategy idleStrategy;

    private SharedWorker(SharedState sharedState) {
      this.sharedState = sharedState;
      this.running = sharedState.running;
      this.idleStrategy = sharedState.idleStrategy;
    }

    @Override
    public void run() {
      while (true) {
        if (sharedState.subscribers.isEmpty()) {
          idleStrategy.idle();
          continue;
        }

        DataStreamSubscriber subscriber = sharedState.subscribers.get(0);
        subscriber.request();
        Object currentNext = subscriber.currentNext();

        if (currentNext == null && !running.get()) {
          break;
        }

        if (currentNext == null) {
          idleStrategy.idle();
        } else {
          HISTOGRAM.recordValue(System.nanoTime() - (long) currentNext);
        }
      }
    }
  }

  private static void report(Object ignored) {
    System.out.println("---- PING/PONG HISTO ----");
    HISTOGRAM.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1.0, false);
    System.out.println("---- PING/PONG HISTO ----");
  }

  private static Disposable startReport() {
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL))
        .doOnNext(OutboundModelBenchmarkRunner::report)
        .doFinally(OutboundModelBenchmarkRunner::report)
        .subscribe();
  }
}
