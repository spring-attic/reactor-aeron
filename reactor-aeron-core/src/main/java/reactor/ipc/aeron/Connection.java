package reactor.ipc.aeron;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface Connection extends Disposable {

  /**
   * Return an existing {@link Connection} that must match the given type wrapper or null.
   *
   * @param clazz connection type to match to
   * @return a matching {@link Connection} reference or null
   */
  default <T extends Connection> T as(Class<T> clazz) {
    if (clazz.isAssignableFrom(this.getClass())) {
      @SuppressWarnings("unchecked")
      T thiz = (T) this;
      return thiz;
    }
    return null;
  }

  /**
   * Return the {@link AeronInbound} read API from this connection. If {@link Connection} has not
   * been configured with a supporting bridge, receive operations will be unavailable.
   *
   * @return the {@link AeronInbound} read API from this connection.
   */
  AeronInbound inbound();

  /**
   * Return the {@link AeronOutbound} write API from this connection. If {@link Connection} has not
   * been configured with a supporting bridge, send operations will be unavailable.
   *
   * @return the {@link AeronOutbound} read API from this connection.
   */
  AeronOutbound outbound();

  /**
   * Return a Mono succeeding when a {@link Connection} is not used anymore by any current
   * operations.
   *
   * @return a Mono succeeding when a {@link Connection} has been terminated
   */
  Mono<Void> onTerminate();
}
