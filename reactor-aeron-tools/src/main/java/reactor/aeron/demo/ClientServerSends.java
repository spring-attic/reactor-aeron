package reactor.aeron.demo;

import reactor.aeron.AeronResources;
import reactor.aeron.client.AeronClient;

public class ClientServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      AeronClient.create("client", aeronResources)
          .options(
              options -> {
                options.serverChannel(
                    channel -> channel.media("udp").reliable(true).endpoint("localhost:13000"));
                options.clientChannel(
                    channel -> channel.media("udp").reliable(true).endpoint("localhost:12001"));
              })
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
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}
