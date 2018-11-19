package reactor.ipc.aeron.client;

import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Connection;

public class AeronClientDoOn extends AeronClientOperator {

  private final Consumer<? super Connection> onConnected;
  private final Consumer<? super Connection> onDisconnected;

  AeronClientDoOn(
      AeronClient source,
      Consumer<? super Connection> onConnected,
      Consumer<? super Connection> onDisconnected) {
    super(source);
    this.onConnected = onConnected;
    this.onDisconnected = onDisconnected;
  }

  @Override
  public Mono<? extends Connection> connect(AeronClientOptions options) {
    return super.connect(options)
        .doOnSuccess(
            connection -> {
              if (onConnected != null) {
                onConnected.accept(connection);
              }
              if (onDisconnected != null) {
                connection
                    .onTerminate()
                    .doOnTerminate(() -> onDisconnected.accept(connection))
                    .subscribe(
                        null,
                        th -> {
                          // no-op
                        });
              }
            });
  }
}
