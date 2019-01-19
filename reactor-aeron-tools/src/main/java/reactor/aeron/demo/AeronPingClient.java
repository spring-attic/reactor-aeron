package reactor.aeron.demo;

import java.nio.ByteBuffer;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronConnection;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public final class AeronPingClient {

  private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    int count = 1_000_000_000;

    AeronResources resources = new AeronResources().useTmpDir().singleWorker().start().block();

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

    // send current nano time as body
    connection
        .outbound()
        .send(
            Flux.range(1, count)
                .map(
                    i -> {
                      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                      buffer.putLong(System.nanoTime());
                      buffer.flip();
                      return buffer;
                    }))
        .then()
        .doOnTerminate(() -> System.out.println("---- Sent " + count + " messages. ---- "))
        .doOnTerminate(connection::dispose)
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    // receive back sent start time and update the histogram
    connection
        .inbound()
        .receive()
        .doOnNext(
            buffer -> {
              long start = buffer.getLong();
              long diff = System.nanoTime() - start;
              histogram.recordValue(diff);
            })
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    connection.onDispose(resources).onDispose().block();
  }
}
