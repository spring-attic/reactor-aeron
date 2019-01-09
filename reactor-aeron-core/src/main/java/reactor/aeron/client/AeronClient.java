package reactor.aeron.client;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronChannelUri;
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
   * @return new {@code AeronClient}
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
    return Mono.defer(() -> new AeronClientConnector(op.apply(options)).start());
  }

  /**
   * Setting up {@link AeronClient} options.
   *
   * @param op unary opearator for performing setup of options
   * @return new {@code AeronClient} with applied options
   */
  public AeronClient options(UnaryOperator<AeronOptions> op) {
    return new AeronClient(op.apply(options));
  }

  /**
   * Shortcut client settings.
   *
   * @param address server address
   * @param port server port
   * @param controlPort server control port
   * @return new {@code AeronClient} with applied options
   */
  public AeronClient options(String address, int port, int controlPort) {
    return new AeronClient(options)
        .options(
            opts -> {
              AeronChannelUri inboundUri = opts.inboundUri();
              AeronChannelUri outboundUri = opts.outboundUri();
              return opts //
                  .outboundUri(outboundUri.endpoint(address + ':' + port)) // Pub
                  .inboundUri(
                      inboundUri
                          .controlEndpoint(address + ':' + controlPort)
                          .controlModeDynamic()); // Sub->MDC(sessionId)
            });
  }

  /**
   * Attach IO handler to react on connected client.
   *
   * @param handler IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return new {@code AeronClient} with handler
   */
  public AeronClient handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronClient(options.handler(handler));
  }
}
