package reactor.ipc.aeron.client;

import io.aeron.driver.AeronResources;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Connection;

class AeronClientConnectionProvider implements Disposable {

  private final String name;
  private final AeronResources aeronResources;
  private final List<AeronClientConnector> connectors = new CopyOnWriteArrayList<>();

  AeronClientConnectionProvider(String name, AeronResources aeronResources) {
    this.name = name;
    this.aeronResources = aeronResources;
  }

  /**
   * Return a new {@link Connection} on subscribe.
   *
   * @param options the aeron client options
   * @return a new {@link Mono} of {@link Connection}
   */
  public Mono<? extends Connection> acquire(AeronClientOptions options) {
    return Mono.defer(
        () -> {
          AeronClientConnector connector =
              new AeronClientConnector(name, aeronResources, options);
          connectors.add(connector);
          return connector
              .newHandler()
              .doOnSuccess(
                  connection ->
                      connection
                          .onDispose()
                          .doOnTerminate(
                              () -> {
                                connectors.removeIf(client -> client == connector);
                                connector.dispose();
                              }));
        });
  }

  @Override
  public void dispose() {
    connectors.forEach(AeronClientConnector::dispose);
  }

  @Override
  public boolean isDisposed() {
    return connectors.isEmpty();
  }
}
