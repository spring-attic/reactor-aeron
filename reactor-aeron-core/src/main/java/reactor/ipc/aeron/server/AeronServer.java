package reactor.ipc.aeron.server;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.OnDisposable;

public abstract class AeronServer {

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
    return new AeronServerBind(name, aeronResources);
  }

  public final Mono<? extends OnDisposable> bind() {
    return bind(options());
  }

  public abstract Mono<? extends OnDisposable> bind(AeronOptions options);

  protected AeronOptions options() {
    return new AeronOptions();
  }

  /**
   * Apply {@link AeronOptions} on the given options consumer to be ultimately used for for invoking
   * {@link #options()}.
   *
   * @param options a consumer aeron server options
   * @return a new {@link AeronServer}
   */
  public AeronServer options(Consumer<AeronOptions> options) {
    Objects.requireNonNull(options, "options");
    AeronOptions aeronOptions = new AeronOptions();
    options.accept(aeronOptions);
    return new AeronServerOnOptions(this, aeronOptions);
  }

  /**
   * Setup a callback called when server is bound.
   *
   * @param doOnBound a consumer observing server started event
   * @return a new {@link AeronServer}
   */
  public final AeronServer doOnBound(Consumer<? super Disposable> doOnBound) {
    Objects.requireNonNull(doOnBound, "doOnBound");
    return new AeronServerDoOn(this, doOnBound, null, null);
  }

  /**
   * Setup a callback called when a remote client is connected.
   *
   * @param doOnConnection a consumer observing connected clients
   * @return a new {@link AeronServer}
   */
  public final AeronServer doOnConnection(Consumer<? super Connection> doOnConnection) {
    Objects.requireNonNull(doOnConnection, "doOnConnection");
    return new AeronServerDoOn(this, null, null, doOnConnection);
  }

  /**
   * Setup a callback called when server is unbound.
   *
   * @param doOnUnbound a consumer observing server dispose event
   * @return a new {@link AeronServer}
   */
  public final AeronServer doOnUnbound(Consumer<? super Disposable> doOnUnbound) {
    Objects.requireNonNull(doOnUnbound, "doOnUnbound");
    return new AeronServerDoOn(this, null, doOnUnbound, null);
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronServer}
   */
  public final AeronServer handle(
      BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> handler) {
    Objects.requireNonNull(handler, "handler");
    return new AeronServerHandle(this, handler);
  }
}
