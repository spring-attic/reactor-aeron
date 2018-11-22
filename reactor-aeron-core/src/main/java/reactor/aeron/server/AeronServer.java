package reactor.aeron.server;

import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;

public final class AeronServer {

  private final AeronServerSettings settings;

  private AeronServer(AeronServerSettings settings) {
    this.settings = settings;
  }

  /**
   * Create aeron server.
   *
   * @param aeronResources aeronResources
   * @return aeron server
   */
  public static AeronServer create(AeronResources aeronResources) {
    return create("server", aeronResources);
  }

  /**
   * Create aeron server.
   *
   * @param name name
   * @param aeronResources aeronResources
   * @return aeron server
   */
  public static AeronServer create(String name, AeronResources aeronResources) {
    return new AeronServer(
        AeronServerSettings.builder().name(name).aeronResources(aeronResources).build());
  }

  public Mono<? extends OnDisposable> bind() {
    return bind(settings.options());
  }

  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return Mono.fromCallable(() -> new ServerHandler(settings.options(options)));
  }

  /**
   * Apply {@link AeronOptions} on the given options consumer.
   *
   * @param options a consumer aeron server options
   * @return a new {@link AeronServer}
   */
  public AeronServer options(Consumer<AeronOptions> options) {
    return new AeronServer(settings.options(options));
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronServer}
   */
  public AeronServer handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronServer(settings.handler(handler));
  }
}
