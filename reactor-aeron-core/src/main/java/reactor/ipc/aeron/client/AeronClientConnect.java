package reactor.ipc.aeron.client;

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Connection;

class AeronClientConnect extends AeronClient {

  private final AeronClientConnectionProvider connectionProvider;

  AeronClientConnect(AeronClientConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @Override
  public Mono<? extends Connection> connect(AeronClientOptions options) {
    return connectionProvider.acquire(options);
  }
}
