package reactor.ipc.aeron.server;

import io.aeron.driver.AeronResources;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.Connection;
import reactor.ipc.aeron.OnDisposable;

public class AeronServerSettings {

  private final String name;
  private final AeronResources aeronResources;
  private final AeronOptions options;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  private AeronServerSettings(Builder builder) {
    this.name = builder.name;
    this.aeronResources = builder.aeronResources;
    this.options = builder.options;
    this.handler = builder.handler;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String name() {
    return name;
  }

  public AeronResources aeronResources() {
    return aeronResources;
  }

  public AeronOptions options() {
    return options;
  }

  public AeronServerSettings options(Consumer<AeronOptions> consumer) {
    return new Builder(this).options(consumer).build();
  }

  public AeronServerSettings options(AeronOptions options) {
    return new Builder(this).options(options).build();
  }

  public Function<? super Connection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronServerSettings handler(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new Builder(this).handler(handler).build();
  }

  public static class Builder {

    private String name;
    private AeronResources aeronResources;
    private AeronOptions options = new AeronOptions();
    private Function<? super Connection, ? extends Publisher<Void>> handler =
        OnDisposable::onDispose;

    private Builder() {}

    private Builder(AeronServerSettings settings) {
      this.name = settings.name;
      this.aeronResources = settings.aeronResources;
      this.options = settings.options;
      this.handler = settings.handler;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder aeronResources(AeronResources aeronResources) {
      this.aeronResources = aeronResources;
      return this;
    }

    public Builder options(AeronOptions options) {
      this.options = options;
      return this;
    }

    public Builder options(Consumer<AeronOptions> consumer) {
      consumer.accept(options);
      return this;
    }

    public Builder handler(Function<? super Connection, ? extends Publisher<Void>> handler) {
      this.handler = handler;
      return this;
    }

    public AeronServerSettings build() {
      return new AeronServerSettings(this);
    }
  }
}
