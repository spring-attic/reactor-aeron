package reactor.aeron.demo.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Random;
import reactor.aeron.demo.Configurations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public final class RsocketTcpServerTps {

  private static final ByteBuf BUFFER =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  static {
    Random random = new Random(System.nanoTime());
    byte[] bytes = new byte[Configurations.MESSAGE_LENGTH];
    random.nextBytes(bytes);
    BUFFER.writeBytes(bytes);
  }

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
                      public Flux<Payload> requestStream(Payload payload) {
                        payload.release();

                        return Mono.fromCallable(() -> ByteBufPayload.create(BUFFER.retain()))
                            .repeat(Configurations.NUMBER_OF_MESSAGES)
                            .subscribeOn(Schedulers.single());
                      }
                    }))
        .transport(() -> TcpServerTransport.create(tcpServer))
        .start()
        .block()
        .onClose()
        .block();
  }
}
