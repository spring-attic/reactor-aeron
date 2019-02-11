package reactor.aeron.demo.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.util.Random;
import reactor.aeron.demo.Configurations;
import reactor.core.publisher.Flux;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public class ReactorNettyClientTps {

  private static final ByteBuf PAYLOAD =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  static {
    Random random = new Random(System.nanoTime());
    byte[] bytes = new byte[Configurations.MESSAGE_LENGTH];
    random.nextBytes(bytes);
    PAYLOAD.writeBytes(bytes);
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    System.out.println(
        "address: " + Configurations.MDC_ADDRESS + ", port: " + Configurations.MDC_PORT);

    LoopResources loopResources = LoopResources.create("reactor-netty-ping");

    TcpClient.create(ConnectionProvider.newConnection())
        .addressSupplier(
            () ->
                InetSocketAddress.createUnresolved(
                    Configurations.MDC_ADDRESS, Configurations.MDC_PORT))
        .runOn(loopResources)
        .bootstrap(
            b ->
                BootstrapHandlers.updateConfiguration(
                    b,
                    "channel",
                    (connectionObserver, channel) -> {
                      setupChannel(channel);
                    }))
        .doOnConnected(c -> System.out.println("connected"))
        .handle(
            (inbound, outbound) ->
                outbound
                    .options(SendOptions::flushOnEach)
                    .send(
                        Flux.range(0, Byte.MAX_VALUE)
                            .repeat(Integer.MAX_VALUE)
                            .map(i -> PAYLOAD.retainedSlice()))
                    .then())
        .connectNow()
        .onDispose()
        .doFinally(s -> loopResources.dispose())
        .block();
  }

  private static void setupChannel(Channel channel) {
    final int maxFrameLength = 1024 * 1024;
    final int lengthFieldLength = 2;

    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast(new LengthFieldPrepender(lengthFieldLength));
    pipeline.addLast(
        new LengthFieldBasedFrameDecoder(
            maxFrameLength, 0, lengthFieldLength, 0, lengthFieldLength));
  }
}
