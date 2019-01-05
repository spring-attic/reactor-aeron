package reactor.aeron.client;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
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
   * Creates {@link AeronClient}.
   *
   * @param resources aeron resources
   * @return new {@link AeronClient}
   */
  public static AeronClient create(AeronResources resources) {
    return new AeronClient(new AeronOptions().resources(resources));
  }

  /**
   * Connects {@link AeronClient}.
   *
   * @return mono handle of result
   */
  public Mono<? extends Connection> connect() {
    return connect(s -> s);
  }

  /**
   * Connects {@link AeronClient} with options.
   *
   * @param op unary opearator for performing setup of options
   * @return mono handle of result
   */
  public Mono<? extends Connection> connect(UnaryOperator<AeronOptions> op) {
    return Mono.defer(() -> connect0(op.apply(options)));
  }

  private Mono<? extends Connection> connect0(AeronOptions options) {
    return Mono.defer(
        () -> {
          AeronClientConnector clientConnector = new AeronClientConnector(options);

          return clientConnector
              .start()
              .doOnError(
                  ex -> {
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
                              clientConnector.dispose();
                            })
                        .subscribe(
                            null,
                            th -> {
                              // no-op
                            });
                  });
        });
  }

  /**
   * Setting up {@link AeronClient} options.
   *
   * @param op unary opearator for performing setup of options
   * @return new {@link AeronClient} with applied options
   */
  public AeronClient options(UnaryOperator<AeronOptions> op) {
    return new AeronClient(op.apply(options));
  }

  /**
   * Attach IO handler to react on connected client.
   *
   * @param handler IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return new {@link AeronClient} with handler
   */
  public AeronClient handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronClient(options.handler(handler));
  }
}
