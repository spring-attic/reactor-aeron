package reactor.aeron.demo;

import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public class ServerDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources resources = new AeronResources().useTmpDir().start().block();
    try {
      AeronServer.create(resources)
          .options("localhost", 13000, 13001)
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
    } finally {
      resources.dispose();
      resources.onDispose().block();
    }
  }
}
