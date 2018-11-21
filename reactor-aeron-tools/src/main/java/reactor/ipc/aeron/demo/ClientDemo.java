package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import reactor.core.publisher.Mono;
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
                  (inbound, outbound) -> {
                    System.out.println("Handler invoked");
                    outbound
                        .send(ByteBufferFlux.from("Hello", "world!").log("send"))
                        .then()
                        .subscribe(
                            avoid -> {
                              // no-op
                            },
                            Throwable::printStackTrace);
                    return Mono.never();
                  })
              .connect()
              .block();
    } finally {
      Objects.requireNonNull(connection).dispose();
    }
    System.out.println("main completed");
  }
}
