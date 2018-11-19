package reactor.ipc.aeron.client;

import io.aeron.driver.AeronResources;
import java.util.Objects;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Connection;

public class AeronClientOperator extends AeronClient {

  private final AeronClient source;

  public AeronClientOperator(AeronClient source) {
    this.source = Objects.requireNonNull(source, "source");
  }

  public static AeronClient create(String name, AeronResources aeronResources) {
    return AeronClient.create(name, aeronResources);
  }

  @Override
  public Mono<? extends Connection> connect(AeronClientOptions options) {
    return source.connect(options);
  }

  @Override
  public AeronClientOptions options() {
    return source.options();
  }

  @Override
  public AeronClient options(Consumer<AeronClientOptions> options) {
    return source.options(options);
  }
}
