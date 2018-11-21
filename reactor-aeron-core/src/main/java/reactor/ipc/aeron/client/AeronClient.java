package reactor.ipc.aeron.client;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.Connection;

public abstract class AeronClient {

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
    AeronClientConnectionProvider connectionProvider =
        new AeronClientConnectionProvider(name, aeronResources);
    return new AeronClientConnect(connectionProvider);
  }

  public final Mono<? extends Connection> connect() {
    return connect(options());
  }

  public abstract Mono<? extends Connection> connect(AeronClientOptions options);

  protected AeronClientOptions options() {
    return new AeronClientOptions();
  }

  /**
   * Apply {@link AeronClientOptions} on the given options consumer to be ultimately used for for
   * invoking {@link #options()}.
   *
   * @param options a consumer aeron client options
   * @return a new {@link AeronClient}
   */
  public AeronClient options(Consumer<AeronClientOptions> options) {
    Objects.requireNonNull(options, "options");
    AeronClientOptions aeronClientOptions = new AeronClientOptions();
    options.accept(aeronClientOptions);
    return new AeronClientOnOptions(this, aeronClientOptions);
  }

  /**
   * Setup a callback called after {@link AeronClient} has been connected.
   *
   * @param doOnConnected a consumer observing connected events
   * @return a new {@link AeronClient}
   */
  public final AeronClient doOnConnected(Consumer<? super Connection> doOnConnected) {
    Objects.requireNonNull(doOnConnected, "doOnConnected");
    return new AeronClientDoOn(this, doOnConnected, null);
  }

  /**
   * Setup a callback called after {@link Connection} has been disconnected.
   *
   * @param doOnDisconnected a consumer observing disconnected events
   * @return a new {@link AeronClient}
   */
  public final AeronClient doOnDisconnected(Consumer<? super Connection> doOnDisconnected) {
    Objects.requireNonNull(doOnDisconnected, "doOnDisconnected");
    return new AeronClientDoOn(this, null, doOnDisconnected);
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronClient}
   */
  public final AeronClient handle(
      BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> handler) {
    Objects.requireNonNull(handler, "handler");
    return doOnConnected(
        connection ->
            Mono.fromDirect(handler.apply(connection.inbound(), connection.outbound()))
                .subscribe(connection.disposeSubscriber()));
  }
}
