package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronConnection;
import reactor.aeron.AeronResources;
import reactor.aeron.DirectBufferHandler;
import reactor.core.publisher.Flux;

public final class AeronPingClient {

  private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(4)
            .singleWorker()
            // .workerIdleStrategySupplier(YieldingIdleStrategy::new)
            .media(ctx -> ctx.threadingMode(ThreadingMode.SHARED))
            .start()
            .block();

    AeronConnection connection =
        AeronClient.create(resources).options("localhost", 13000, 13001).connect().block();

    // start reporter
    Recorder histogram = new Recorder(3600000000000L, 3);
    Flux.interval(REPORT_INTERVAL)
        .doOnNext(
            i -> {
              System.out.println("---- PING/PONG HISTO ----");
              histogram
                  .getIntervalHistogram()
                  .outputPercentileDistribution(System.out, 5, 1000.0, false);
              System.out.println("---- PING/PONG HISTO ----");
            })
        .subscribe();

    NanoTimeGeneratorHandler handler = new NanoTimeGeneratorHandler();

    connection.outbound().send(Flux.range(0, 8), handler).then().subscribe();

    connection
        .outbound()
        .send(
            connection
                .inbound()
                .receive()
                .doOnNext(
                    buffer -> {
                      long start = buffer.getLong(0);
                      long diff = System.nanoTime() - start;
                      histogram.recordValue(diff);
                    }),
            handler)
        .then()
        .subscribe();

    connection.onDispose(resources).onDispose().block();
  }

  private static class NanoTimeGeneratorHandler implements DirectBufferHandler<Object> {
    @Override
    public int estimateLength(Object ignore) {
      return Long.BYTES;
    }

    @Override
    public void write(MutableDirectBuffer dstBuffer, int index, Object ignore, int length) {
      dstBuffer.putLong(index, System.nanoTime());
    }

    @Override
    public DirectBuffer map(Object ignore, int length) {
      UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
      buffer.putLong(0, System.nanoTime());
      return buffer;
    }

    @Override
    public void dispose(Object ignore) {}
  }
}
