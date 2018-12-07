package reactor.aeron.server;

import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;

public final class AeronServer {

  private static final int CONTROL_STREAM_ID = 1;

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

  /**
   * Binds server with given options.
   *
   * @param options server options
   * @return mono handle of result
   */
  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return Mono.defer(
        () -> {
          AeronServerSettings settings = this.settings.options(options);
          AeronServerHandler serverHandler = new AeronServerHandler(settings);

          AeronResources resources = settings.aeronResources();
          String category = settings.name();
          String serverChannel = settings.options().serverChannel();
          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .controlSubscription(
                  category,
                  serverChannel,
                  CONTROL_STREAM_ID,
                  serverHandler,
                  eventLoop,
                  null,
                  null /**/)
              .map(
                  controlSubscription -> {
                    serverHandler.onSubscription(controlSubscription);
                    serverHandler
                        .onDispose()
                        .doFinally(s -> controlSubscription.dispose())
                        .subscribe(
                            null,
                            ex -> {
                              // no-op
                            });
                    return serverHandler;
                  });
        });
  }

  /**
   * Apply {@link AeronOptions} on the given options consumer.
   *
   * @param options a consumer aeron server options
   * @return a new {@link AeronServer}
   */
  public AeronServer options(Consumer<AeronOptions.Builder> options) {
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
