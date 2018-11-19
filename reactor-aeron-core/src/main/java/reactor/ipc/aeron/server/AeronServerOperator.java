package reactor.ipc.aeron.server;

import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.OnDisposable;

class AeronServerOperator extends AeronServer {
  private final AeronServer aeronServer;

  AeronServerOperator(AeronServer aeronServer) {
    this.aeronServer = aeronServer;
  }

  @Override
  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return aeronServer.bind(options);
  }

  @Override
  public AeronOptions options() {
    return aeronServer.options();
  }

  @Override
  public AeronServer options(Consumer<AeronOptions> options) {
    return aeronServer.options(options);
  }
}
