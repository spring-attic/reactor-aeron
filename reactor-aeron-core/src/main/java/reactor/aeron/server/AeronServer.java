package reactor.aeron.server;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;

public final class AeronServer {

  private final AeronOptions options;

  private AeronServer(AeronOptions options) {
    this.options = options;
  }

  /**
   * Create aeron server.
   *
   * @param resources aeron resources
   * @return aeron server
   */
  public static AeronServer create(AeronResources resources) {
    return new AeronServer(new AeronOptions().resources(resources));
  }

  /**
   * Binds aeron server.
   *
   * @return mono handle of result
   */
  public Mono<? extends OnDisposable> bind() {
    return bind(s -> s);
  }

  /**
   * Binds aeron server and applies server options.
   *
   * @param o unary opearator for performing setup of options
   * @return mono handle of result
   */
  public Mono<? extends OnDisposable> bind(UnaryOperator<AeronOptions> o) {
    return Mono.defer(() -> new AeronServerHandler(o.apply(options)).start());
  }

  /**
   * Setting up server options.
   *
   * @param o unary opearator for performing setup of options
   * @return aeron server with applied options
   */
  public AeronServer options(UnaryOperator<AeronOptions> o) {
    return new AeronServer(o.apply(options));
  }

  /**
   * Attach IO handler to react on connected client.
   *
   * @param handler IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return new {@link AeronServer}
   */
  public AeronServer handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronServer(options.handler(handler));
  }
}
