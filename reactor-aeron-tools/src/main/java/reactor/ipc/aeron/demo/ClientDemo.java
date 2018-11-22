package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.client.AeronClient;

public class ClientDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {

    Connection connection = null;
    try (AeronResources aeronResources = AeronResources.start()) {

      connection =
          AeronClient.create("client", aeronResources)
              .options(
                  options -> {
                    options.serverChannel("aeron:udp?endpoint=localhost:13000");
                    options.clientChannel("aeron:udp?endpoint=localhost:12001");
                  })
              .handle(
                  connection1 -> {
                    System.out.println("Handler invoked");
                    return connection1
                        .outbound()
                        .send(ByteBufferFlux.from("Hello", "world!").log("send"))
                        .then(connection1.onDispose());
                  })
              .connect()
              .block();
    } finally {
      Objects.requireNonNull(connection).dispose();
    }
    System.out.println("main completed");
  }
}
