package reactor.ipc.aeron.demo;

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.server.AeronServer;

public class ServerDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronServer server =
        AeronServer.create(
            "server",
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
  }
}
