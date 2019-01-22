package reactor.aeron.demo;

import java.nio.ByteBuffer;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughput {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = new AeronResources().useTmpDir().start().block();
    try {
      ByteBuffer buffer = ByteBuffer.allocate(1024);

      AeronClient.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .outbound()
                      .sendBuffer(Flux.range(0, Integer.MAX_VALUE).map(i -> buffer))
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
