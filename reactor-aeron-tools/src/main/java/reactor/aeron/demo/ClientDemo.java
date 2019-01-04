package reactor.aeron.demo;

import java.util.Objects;
import reactor.aeron.AeronResources;
import reactor.aeron.ByteBufferFlux;
import reactor.aeron.Connection;
import reactor.aeron.client.AeronClient;

public class ClientDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    Connection connection = null;
    AeronResources aeronResources = AeronResources.start();
    try {
      connection =
          AeronClient.create(aeronResources)
              .options(
                  options -> {
                    options.serverChannel(
                        channel -> channel.media("udp").reliable(true).endpoint("localhost:13000"));
                    options.clientChannel(
                        channel -> channel.media("udp").reliable(true).endpoint("localhost:12001"));
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
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
    System.out.println("main completed");
  }
}
