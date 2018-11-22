package reactor.ipc.aeron.demo;

import io.aeron.driver.AeronResources;
import reactor.ipc.aeron.client.AeronClient;

public class ClientServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {

    try (AeronResources aeronResources = AeronResources.start()) {

      AeronClient.create("client", aeronResources)
          .options(
              options -> {
                options.serverChannel("aeron:udp?endpoint=localhost:13000");
                options.clientChannel("aeron:udp?endpoint=localhost:12001");
              })
          .doOnConnected(
              connection -> System.out.println(connection + " was connected successfully"))
          .doOnDisconnected(connection -> System.out.println(connection + " was disconnected"))
          .connect()
          .doOnSuccess(
              connection -> connection.inbound().receive().asString().log("receive").subscribe())
          .block();

      System.out.println("main completed");
      Thread.currentThread().join();
    }
  }
}
