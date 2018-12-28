package reactor.aeron.demo;

import reactor.aeron.AeronResources;
import reactor.aeron.server.AeronServer;

public class ServerDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      AeronServer.create("server", aeronResources)
          .options(
              options ->
                  options.serverChannel(
                      channel -> channel.media("udp").reliable(true).endpoint("localhost:13000")))
          .handle(
              connection ->
                  connection
                      .inbound()
                      .receiveAsString()
                      .log("receive")
                      .then(connection.onDispose()))
          .bind()
          .block();

      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}
