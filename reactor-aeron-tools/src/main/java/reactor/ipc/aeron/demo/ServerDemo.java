package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.server.AeronServer;

public class ServerDemo {

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
                options.serverChannel("aeron:udp?endpoint=localhost:13000");
              });
      server
          .newHandler(
              (inbound, outbound) -> {
                inbound.receive().asString().log("receive").subscribe();
                return Mono.never();
              })
          .block();

      Thread.currentThread().join();
    }
  }
}
