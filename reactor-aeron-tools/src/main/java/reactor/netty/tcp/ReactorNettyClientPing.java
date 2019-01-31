package reactor.netty.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public class ReactorNettyClientPing {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress(7071);

  private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

  public static void main(String[] args) {
    LoopResources loopResources = LoopResources.create("client", 1, 4, true);

    Connection connection =
        TcpClient.create(ConnectionProvider.fixed("client-provider", 4))
            .addressSupplier(() -> ADDRESS)
            .runOn(loopResources)
            .doOnConnected(c -> System.out.println("connected"))
            .connectNow();

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
        .options(SendOptions::flushOnEach)
        .send(Flux.range(0, Integer.MAX_VALUE).map(i -> generatePayload()))
        .then()
        .doOnTerminate(connection::dispose)
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    // receive back sent start time and update the histogram
    connection
        .inbound()
        .receive()
        .retain()
        .doOnNext(
            buffer -> {
              long start = buffer.getLong(0);
              long diff = System.nanoTime() - start;
              histogram.recordValue(diff);
              buffer.release();
            })
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    connection.onDispose(loopResources).onDispose().block();
  }

  private static ByteBuf generatePayload() {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(Long.BYTES);
    buffer.writeLong(System.nanoTime());
    return buffer;
  }
}
