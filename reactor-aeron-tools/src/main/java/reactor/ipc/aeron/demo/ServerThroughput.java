package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.server.AeronServer;

public class ServerThroughput {

  static final String HOST = "localhost";

  static final int SLIDING_AVG_DURATION_SEC = 5;

  static class Data {

    private final long time;

    private final int size;

    Data(long time, int size) {
      this.time = time;
      this.size = size;
    }
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {

    try (AeronResources aeronResources = AeronResources.start()) {

      AeronServer server =
          AeronServer.create(
              "server",
              aeronResources,
              options -> {
                options.serverChannel("aeron:udp?endpoint=" + HOST + ":13000");
              });

      Queue<Data> queue = new ConcurrentLinkedDeque<>();
      AtomicLong counter = new AtomicLong();
      Schedulers.single()
          .schedulePeriodically(
              () -> {
                long end = now();
                long cutoffTime = end - TimeUnit.SECONDS.toMillis(SLIDING_AVG_DURATION_SEC);

                long value = counter.getAndSet(0);
                long total = 0;
                Iterator<Data> it = queue.iterator();
                while (it.hasNext()) {
                  Data data = it.next();
                  if (data.time < cutoffTime) {
                    it.remove();
                  } else {
                    total += data.size;
                  }
                }

                System.out.printf(
                    "Rate: %d MB/s, %ds avg rate: %d MB/s\n",
                    toMb(value), SLIDING_AVG_DURATION_SEC, toMb(total) / SLIDING_AVG_DURATION_SEC);
              },
              1,
              1,
              TimeUnit.SECONDS);

      server
          .newHandler(
              (inbound, outbound) -> {
                inbound
                    .receive()
                    .doOnNext(
                        buffer -> {
                          int size = buffer.remaining();
                          queue.add(new Data(now(), size));
                          counter.addAndGet(size);
                        })
                    .subscribe();
                return Mono.never();
              })
          .block();

      Thread.currentThread().join();
    }
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static long toMb(long value) {
    return value / (1024 * 1024);
  }
}
