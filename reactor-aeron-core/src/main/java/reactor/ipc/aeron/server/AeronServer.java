package reactor.ipc.aeron.server;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;

/** Aeron server. */
public final class AeronServer implements AeronConnector {

  private final AeronOptions options;

  private final String name;

  private final AeronResources aeronResources;

  /**
   * Create aeron server.
   *
   * @param aeronResources aeronResources
   * @return aeron server
   */
  public static AeronServer create(AeronResources aeronResources) {
    return create(
        null,
        aeronResources,
        options -> {
          // no-op
        });
  }

  public static AeronServer create(
      String name, AeronResources aeronResources, Consumer<AeronOptions> optionsConfigurer) {
    return new AeronServer(name, aeronResources, optionsConfigurer);
  }

  private AeronServer(
      String name, AeronResources aeronResources, Consumer<AeronOptions> optionsConfigurer) {
    this.name = name == null ? "server" : name;
    this.aeronResources = aeronResources;
    AeronOptions options = new AeronOptions();
    optionsConfigurer.accept(options);
    this.options = options;
  }

  @Override
  public Mono<? extends Disposable> newHandler(
      BiFunction<AeronInbound, AeronOutbound, ? extends Publisher<Void>> ioHandler) {
    Objects.requireNonNull(ioHandler, "ioHandler shouldn't be null");

    return Mono.create(
        sink -> {
          ServerHandler handler = new ServerHandler(name, ioHandler, aeronResources, options);
          sink.success(handler);
        });
  }
}
