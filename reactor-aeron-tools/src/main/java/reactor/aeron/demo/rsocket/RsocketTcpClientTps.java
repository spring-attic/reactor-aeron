package reactor.aeron.demo.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import reactor.aeron.demo.Configurations;
import reactor.aeron.demo.RateReporter;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class RsocketTcpClientTps {

  private static final ByteBuf BUFFER =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  private static final Payload PAYLOAD =
      ByteBufPayload.create(ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH));

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    TcpClient tcpClient =
        TcpClient.create(ConnectionProvider.elastic("tcp-client"))
            .runOn(LoopResources.create("client", 1, true))
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT);

    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start()
            .block();

    RateReporter reporter = new RateReporter();

    Payload request = ByteBufPayload.create("hello");

    client
        .requestStream(request)
        .doOnNext(
            payload -> {
              reporter.onMessage(1, payload.sliceData().readableBytes());
              payload.release();
            })
        .doOnError(Throwable::printStackTrace)
        .doFinally(s -> reporter.dispose())
        .then()
        .block();
  }
}
