package reactor.netty.tcp;

import io.netty.channel.ChannelOption;
import java.net.InetSocketAddress;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.resources.LoopResources;

public class ReactorNettyServerPong {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress(7071);

  public static void main(String[] args) {
    LoopResources loopResources = LoopResources.create("server", 1, 4, true);

    TcpServer.create()
        .runOn(loopResources)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .addressSupplier(() -> ADDRESS)
        .handle(
            (inbound, outbound) ->
                outbound.options(SendOptions::flushOnEach).send(inbound.receive().retain()))
        .bind()
        .doOnSuccess(
            server ->
                System.out.println("server has been started successfully on " + server.address()))
        .block()
        .onDispose(loopResources)
        .onDispose()
        .block();
  }
}
