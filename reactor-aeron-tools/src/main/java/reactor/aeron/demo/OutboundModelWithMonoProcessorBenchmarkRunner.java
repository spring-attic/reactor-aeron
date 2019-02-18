package reactor.aeron.demo;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;
import org.agrona.hints.ThreadHints;
import reactor.aeron.DirectBufferHandler;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class OutboundModelWithMonoProcessorBenchmarkRunner {

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
    static final int QUEUE_CAPACITY = 8192;

    final AtomicBoolean running = new AtomicBoolean(true);
    final Queue<PublishTask> publishTasks = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    final IdleStrategy idleStrategy = Configurations.idleStrategy();

    Thread workerThread;

    private void start() {
      workerThread = new Thread(new SharedWorker(running, idleStrategy, publishTasks));
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

    final MonoProcessor<Object> onDispose = MonoProcessor.create();
    final Queue<PublishTask> publishTasks;
    final int concurrency = Configurations.CONCURRENCY;
    final int prefetch = Configurations.PREFETCH;
    final int fluxRepeat = Configurations.FLUX_REPEAT;

    private PerThreadState(SharedState sharedState) {
      this.publishTasks = sharedState.publishTasks;
    }

    @Override
    public void run() {
      Mono.fromCallable(System::nanoTime)
          .repeat(fluxRepeat)
          .flatMap(time -> publish(time, publishTasks), concurrency, prefetch)
          .takeUntilOther(onPublicationDispose(onDispose))
          .then()
          .block();
    }
  }

  private static class SharedWorker implements Runnable {

    final AtomicBoolean running;
    final IdleStrategy idleStrategy;
    final Queue<PublishTask> publishTasks;

    private SharedWorker(
        AtomicBoolean running, IdleStrategy idleStrategy, Queue<PublishTask> publishTasks) {
      this.running = running;
      this.idleStrategy = idleStrategy;
      this.publishTasks = publishTasks;
    }

    @Override
    public void run() {
      while (true) {
        PublishTask task = publishTasks.poll();

        if (task == null && !running.get()) {
          break;
        }

        if (task == null) {
          idleStrategy.idle();
        } else {
          task.complete();
        }
      }
    }
  }

  @SuppressWarnings("unused")
  private static class PublishTask {
    static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(8));

    private final Publication publication = null;
    private final DirectBufferHandler bufferHandler = null;
    private MonoProcessor<Void> promise;
    private DirectBuffer buffer = BUFFER;
    private long time;

    private static PublishTask newInstance(MonoProcessor<Void> promise, long time) {
      PublishTask task = new PublishTask();
      task.time = time;
      task.promise = promise;
      return task;
    }

    private void complete() {
      promise.onComplete();
      HISTOGRAM.recordValue(System.nanoTime() - time);
    }
  }

  private static Mono<Void> publish(long time, Queue<PublishTask> publishTasks) {
    return Mono.defer(
        () -> {
          MonoProcessor<Void> promise = MonoProcessor.create();
          while (!publishTasks.offer(PublishTask.newInstance(promise, time))) {
            ThreadHints.onSpinWait();
          }
          return promise;
        });
  }

  private static Mono<Void> onPublicationDispose(MonoProcessor<Object> onDispose) {
    return onDispose.then(
        Mono.defer(() -> Mono.error(new RuntimeException("onPublicationDispose"))));
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
        .doOnNext(OutboundModelWithMonoProcessorBenchmarkRunner::report)
        .doFinally(OutboundModelWithMonoProcessorBenchmarkRunner::report)
        .subscribe();
  }
}
