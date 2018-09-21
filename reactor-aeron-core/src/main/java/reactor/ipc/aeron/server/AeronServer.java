package reactor.ipc.aeron.server;

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

  public static AeronServer create(String name, Consumer<AeronOptions> optionsConfigurer) {
    return new AeronServer(name, optionsConfigurer);
  }

  /**
   * Factory method.
   *
   * @param name name
   * @return aeron server
   */
  public static AeronServer create(String name) {
    return create(
        name,
        options -> {
          // no-op
        });
  }

  /**
   * Factory method.
   *
   * @return aeron server
   */
  public static AeronServer create() {
    return create(null);
  }

  private AeronServer(String name, Consumer<AeronOptions> optionsConfigurer) {
    this.name = name == null ? "server" : name;
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
          ServerHandler handler = new ServerHandler(name, ioHandler, options);
          handler.initialise();

          sink.success(handler);
        });
  }
}
