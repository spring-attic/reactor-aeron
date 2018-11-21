package reactor.ipc.aeron;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface OnDisposable extends Disposable {

  /**
   * Assign a {@link Disposable} to be invoked when the this has been disposed.
   *
   * @return a Mono succeeding when this has been disposed
   */
  Mono<Void> onDispose();
}
