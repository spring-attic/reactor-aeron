package reactor.aeron.demo;

import java.time.Duration;
import reactor.aeron.AeronResources;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.Flux;

public class ServerServerSends {

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
                      .outbound()
                      .send(
                          Flux.range(1, 10000)
                              .delayElements(Duration.ofMillis(250))
                              .map(i -> AeronUtils.stringToByteBuffer("" + i))
                              .log("send"))
                      .then(connection.onDispose()))
          .bind()
          .block();

      System.out.println("main finished");
      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}
