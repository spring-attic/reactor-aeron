package reactor.aeron.rsocket.netty;

import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.aeron.Configurations;
import reactor.aeron.RateReporter;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class RSocketNettyClientTps {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    TcpClient tcpClient =
        TcpClient.create(ConnectionProvider.newConnection())
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
