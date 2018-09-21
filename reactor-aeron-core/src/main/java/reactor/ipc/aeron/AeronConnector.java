package reactor.ipc.aeron;

import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface AeronConnector {

  /**
   * Prepare a {@link BiFunction} IO handler that will react on a new connected state each time the
   * returned {@link Mono} is subscribed.
   *
   * <p>The IO handler will return {@link Publisher} to signal when to terminate the underlying
   * resource channel.
   *
   * @param ioHandler the in/out callback returning a closing publisher
   * @return a {@link Mono} completing with a {@link Disposable} token to dispose the active handler
   *     (server, client connection...) or failing with the connection error.
   */
  Mono<? extends Disposable> newHandler(
      BiFunction<AeronInbound, AeronOutbound, ? extends Publisher<Void>> ioHandler);
}
