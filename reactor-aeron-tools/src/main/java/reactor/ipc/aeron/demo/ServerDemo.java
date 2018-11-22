package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import reactor.ipc.aeron.server.AeronServer;

public class ServerDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    try (AeronResources aeronResources = AeronResources.start()) {

      AeronServer.create("server", aeronResources)
          .options(options -> options.serverChannel("aeron:udp?endpoint=localhost:13000"))
          .handle(
              connection ->
                  connection
                      .inbound()
                      .receive()
                      .asString()
                      .log("receive")
                      .then(connection.onDispose()))
          .bind()
          .block();

      Thread.currentThread().join();
    }
  }
}
