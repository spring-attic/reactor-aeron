package reactor.aeron.client;

import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.core.publisher.Mono;

public final class AeronClient {

  private final AeronClientSettings settings;

  private AeronClient(AeronClientSettings settings) {
    this.settings = settings;
  }

  /**
   * Create aeron client.
   *
   * @param aeronResources aeronResources
   * @return aeron client
   */
  public static AeronClient create(AeronResources aeronResources) {
    return create("client", aeronResources);
  }

  /**
   * Create aeron client.
   *
   * @param name name
   * @param aeronResources aeronResources
   * @return aeron client
   */
  public static AeronClient create(String name, AeronResources aeronResources) {
    return new AeronClient(
        AeronClientSettings.builder().name(name).aeronResources(aeronResources).build());
  }

  public Mono<? extends Connection> connect() {
    return connect(settings.options());
  }

  public Mono<? extends Connection> connect(AeronClientOptions options) {
    return Mono.defer(() -> connect0(options));
  }

  private Mono<? extends Connection> connect0(AeronClientOptions options) {
    AeronClientConnector connector = new AeronClientConnector(settings.options(options));
    return connector
        .newHandler()
        .doOnError(ex -> connector.dispose())
        .doOnSuccess(
            connection -> {
              settings
                  .handler() //
                  .apply(connection)
                  .subscribe(connection.disposeSubscriber());
              connection
                  .onDispose()
                  .doOnTerminate(connector::dispose)
                  .subscribe(
                      null,
                      th -> {
                        // no-op
                      });
            });
  }

  /**
   * Apply {@link AeronClientOptions} on the given options consumer.
   *
   * @param options a consumer aeron client options
   * @return a new {@link AeronClient}
   */
  public AeronClient options(Consumer<AeronClientOptions.Builder> options) {
    return new AeronClient(settings.options(options));
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronClient}
   */
  public AeronClient handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronClient(settings.handler(handler));
  }
}
