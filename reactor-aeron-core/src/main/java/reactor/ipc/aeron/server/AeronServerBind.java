package reactor.ipc.aeron.server;

import io.aeron.driver.AeronResources;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.OnDisposable;

class AeronServerBind extends AeronServer {

  private final String name;
  private final AeronResources aeronResources;

  AeronServerBind(String name, AeronResources aeronResources) {
    this.name = name;
    this.aeronResources = aeronResources;
  }

  @Override
  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return Mono.create(
        sink -> {
          ServerHandler handler = new ServerHandler(name, aeronResources, options);
          sink.success(handler);
        });
  }
}
