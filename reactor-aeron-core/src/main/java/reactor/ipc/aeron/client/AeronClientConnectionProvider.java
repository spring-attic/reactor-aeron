package reactor.ipc.aeron.client;

import io.aeron.driver.AeronResources;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Connection;

class AeronClientConnectionProvider {

  private final String name;
  private final AeronResources aeronResources;

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
          AeronClientConnector connector = new AeronClientConnector(name, aeronResources, options);
          return connector
              .newHandler()
              .doOnError(th -> connector.dispose())
              .doOnSuccess(
                  connection ->
                      connection
                          .onDispose()
                          .doOnTerminate(connector::dispose)
                          .subscribe(
                              null,
                              th -> {
                                // no-op
                              }));
        });
  }
}
