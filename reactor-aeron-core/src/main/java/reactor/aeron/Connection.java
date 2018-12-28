package reactor.aeron;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;

public interface Connection extends OnDisposable {

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
   * Assign a {@link Disposable} to be invoked when the channel is closed.
   *
   * @param onDispose the close event handler
   * @return {@literal this}
   */
  default Connection onDispose(Disposable onDispose) {
    onDispose().doOnTerminate(onDispose::dispose).subscribe();
    return this;
  }

  /**
   * Return a {@link CoreSubscriber} that will dispose on complete or error.
   *
   * @return a {@link CoreSubscriber} that will dispose on complete or error
   */
  default CoreSubscriber<Void> disposeSubscriber() {
    return new ConnectionDisposer(this);
  }

  final class ConnectionDisposer extends BaseSubscriber<Void> {
    final OnDisposable onDisposable;

    public ConnectionDisposer(OnDisposable onDisposable) {
      this.onDisposable = onDisposable;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(Long.MAX_VALUE);
      onDisposable.onDispose(this);
    }

    @Override
    protected void hookFinally(SignalType type) {
      if (type != SignalType.CANCEL) {
        onDisposable.dispose();
      }
    }
  }
}
