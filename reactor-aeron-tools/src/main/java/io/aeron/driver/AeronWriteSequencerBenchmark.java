package io.aeron.driver;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.DebugUtil;
import reactor.ipc.aeron.DefaultMessagePublication;
import reactor.ipc.aeron.MessagePublication;

public class AeronWriteSequencerBenchmark {

  private final String channel;

  private final int numOfRuns;

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    new AeronWriteSequencerBenchmark("aeron:ipc?endpoint=benchmark", 10).run();
  }

  AeronWriteSequencerBenchmark(String channel, int numOfRuns) {
    this.channel = channel;
    this.numOfRuns = numOfRuns;
  }

  private void run() {
    AeronOptions options = new AeronOptions();
    AeronResources aeronResources = new AeronResources("benchmark");

    io.aeron.Subscription subscription =
        aeronResources.addSubscription("benchmark", channel, 1, "benchmark", 0);

    BenchmarkPooler pooler = new BenchmarkPooler(subscription);
    pooler.schedulePoll();

    Publication publication = aeronResources.publication("benchmark", channel, 1, "benchmark", 0);

    MessagePublication messagePublication =
        new DefaultMessagePublication(
            publication,
            "benchmark",
            options.connectTimeoutMillis(),
            options.backpressureTimeoutMillis());

    AeronWriteSequencer sequencer = aeronResources.writeSequencer("test", messagePublication, 1);

    for (int i = 1; i <= numOfRuns; i++) {
      Publisher<ByteBuffer> publisher = new BenchmarkPublisher(1_000_000, 512);

      long start = System.nanoTime();
      Mono<Void> result = sequencer.add(publisher);
      result.block();
      long end = System.nanoTime();

      System.out.printf(
          "Run %d of %d - completed, took: %d millis\n",
          i, numOfRuns, Duration.ofNanos(end - start).toMillis());
    }

    pooler.dispose();
    aeronResources.close(publication);
    aeronResources.close(subscription);
    aeronResources.dispose();
  }

  private static class BenchmarkPooler implements Disposable {

    private final io.aeron.Subscription subscription;

    private final Scheduler scheduler;

    public BenchmarkPooler(io.aeron.Subscription subscription) {
      this.subscription = subscription;
      this.scheduler = Schedulers.newSingle("drainer");
    }

    void schedulePoll() {
      scheduler.schedule(
          () -> {
            BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

            for (; ; ) {
              if (Thread.currentThread().isInterrupted()) {
                break;
              }

              int numOfPolled =
                  subscription.poll(
                      (buffer, offset, length, header) -> {
                        // no-op
                      },
                      1000);

              idleStrategy.idle(numOfPolled);
            }
          });
    }

    @Override
    public void dispose() {
      scheduler.dispose();
    }
  }

  private static class BenchmarkPublisher implements Publisher<ByteBuffer> {

    final int numOfSignals;

    final int bufferSize;

    private BenchmarkPublisher(int numOfSignals, int bufferSize) {
      this.numOfSignals = numOfSignals;
      this.bufferSize = bufferSize;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
      s.onSubscribe(
          new Subscription() {

            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

            long numOfPublished = 0;

            volatile boolean cancelled = false;

            long requested = 0;

            boolean publishing = false;

            @Override
            public void request(long n) {
              requested += n;

              if (publishing) {
                return;
              }

              publishing = true;
              while (numOfPublished < numOfSignals && requested > 0) {
                if (cancelled) {
                  break;
                }

                s.onNext(buffer);

                numOfPublished += 1;
                requested--;

                if (numOfPublished % (numOfSignals / 10) == 0) {
                  DebugUtil.log("Signals published: " + numOfPublished);
                }

                if (numOfPublished == numOfSignals) {
                  s.onComplete();
                  break;
                }
              }
              publishing = false;
            }

            @Override
            public void cancel() {
              cancelled = true;
            }
          });
    }
  }
}
