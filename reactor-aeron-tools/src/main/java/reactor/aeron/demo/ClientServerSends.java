package reactor.aeron.demo;

import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;

public class ClientServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources resources = new AeronResources().useTmpDir().singleWorker().start().block();
    try {
      AeronClient.create(resources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .inbound()
                      .receive()
                      .asString()
                      .log("receive")
                      .then(connection.onDispose()))
          .connect()
          .block();

      System.out.println("main completed");
      Thread.currentThread().join();
    } finally {
      resources.dispose();
      resources.onDispose().block();
    }
  }
}
