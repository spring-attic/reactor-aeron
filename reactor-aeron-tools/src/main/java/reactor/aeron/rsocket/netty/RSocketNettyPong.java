package reactor.aeron.rsocket.netty;

import io.rsocket.AbstractRSocket;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import reactor.aeron.Configurations;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public final class RSocketNettyPong {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    TcpServer tcpServer =
        TcpServer.create()
            .runOn(LoopResources.create("server", 1, true))
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT);

    RSocketFactory.receive()
        .frameDecoder(Frame::retain)
        .acceptor(
            (setupPayload, rsocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return Mono.just(payload);
                      }
                    }))
        .transport(() -> TcpServerTransport.create(tcpServer))
        .start()
        .block()
        .onClose()
        .block();
  }
}
