package reactor.aeron.demo;

import java.nio.ByteBuffer;
import reactor.aeron.AeronResources;
import reactor.aeron.client.AeronClient;
import reactor.core.publisher.Flux;

public class ClientThroughput {

  private static final String HOST = "localhost";

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      ByteBuffer buffer = ByteBuffer.allocate(1024);

      AeronClient.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .outbound()
                      .send(
                          Flux.create(
                              sink -> {
                                System.out.println("About to send");
                                for (int i = 0; i < 10_000 * 1024; i++) {
                                  sink.next(buffer);
                                }
                                sink.complete();
                                System.out.println("Send complete");
                              }))
                      .then(connection.onDispose()))
          .connect()
          .block();

      System.out.println("main completed");
      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}
