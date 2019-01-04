package reactor.aeron.client;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.core.publisher.Mono;

public final class AeronClient {

  private final AeronOptions options;

  private AeronClient(AeronOptions options) {
    this.options = options;
  }

  /**
   * Create aeron client.
   *
   * @param resources resources
   * @return aeron client
   */
  public static AeronClient create(AeronResources resources) {
    return new AeronClient(new AeronOptions().resources(resources));
  }

  public Mono<? extends Connection> connect() {
    return connect(options.options());
  }

  public Mono<? extends Connection> connect(AeronOptions options) {
    return Mono.defer(() -> connect0(options));
  }

  private Mono<? extends Connection> connect0(AeronOptions options) {
    return Mono.defer(
        () -> {
          AeronClientSettings settings = this.options.options(options);

          int clientControlStreamId = STREAM_ID_COUNTER.incrementAndGet();
          Supplier<Integer> clientSessionStreamIdCounter = STREAM_ID_COUNTER::incrementAndGet;

          AeronClientConnector clientConnector = new AeronClientConnector(settings);

          String category = Optional.ofNullable(settings.name()).orElse("client");
          AeronResources resources = settings.resources();
          String clientChannel = settings.options().clientChannel();
          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .controlSubscription(
                  category,
                  clientChannel,
                  clientControlStreamId,
                  clientConnector,
                  eventLoop,
                  null,
                  image -> clientConnector.dispose())
              .flatMap(
                  controlSubscription -> {
                    clientConnector.onSubscription(controlSubscription);
                    return clientConnector
                        .start()
                        .doOnError(
                            ex -> {
                              controlSubscription.dispose();
                              clientConnector.dispose();
                            })
                        .doOnSuccess(
                            connection -> {
                              settings
                                  .handler() //
                                  .apply(connection)
                                  .subscribe(connection.disposeSubscriber());
                              connection
                                  .onDispose()
                                  .doFinally(
                                      s -> {
                                        controlSubscription.dispose();
                                        clientConnector.dispose();
                                      })
                                  .subscribe(
                                      null,
                                      th -> {
                                        // no-op
                                      });
                            });
                  });
        });
  }

  /**
   * Apply {@link AeronOptions} on the given options consumer.
   *
   * @param options a consumer aeron client options
   * @return a new {@link AeronClient}
   */
  public AeronClient options(Consumer<AeronOptions.Builder> options) {
    return new AeronClient(this.options.options(options));
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronClient}
   */
  public AeronClient handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronClient(options.handler(handler));
  }
}
