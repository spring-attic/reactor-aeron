package reactor.ipc.aeron.server;

import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.OnDisposable;

class AeronServerDoOn extends AeronServerOperator {

  private final Consumer<? super Disposable> onBound;
  private final Consumer<? super Disposable> onUnbound;
  private final Consumer<? super Connection> onConnection;

  AeronServerDoOn(
      AeronServer aeronServer,
      Consumer<? super Disposable> onBound,
      Consumer<? super Disposable> onUnbound,
      Consumer<? super Connection> onConnection) {
    super(aeronServer);
    this.onBound = onBound;
    this.onUnbound = onUnbound;
    this.onConnection = onConnection;
  }

  @Override
  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return super.bind(options)
        .cast(ServerHandler.class)
        .doOnSuccess(
            serverHandler -> {
              if (onBound != null) {
                onBound.accept(serverHandler);
              }
              if (onUnbound != null) {
                serverHandler
                    .onDispose()
                    .doOnTerminate(() -> onUnbound.accept(serverHandler))
                    .subscribe(
                        null,
                        th -> {
                          // no-op
                        });
              }
              if (onConnection != null) {
                serverHandler
                    .connections()
                    .subscribe(
                        onConnection::accept,
                        th -> {
                          // no-op
                        });
              }
            });
  }
}
