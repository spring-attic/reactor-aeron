package reactor.aeron.server;

import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.OnDisposable;

public class AeronServerSettings {

  private final String name;
  private final AeronResources aeronResources;
  private final AeronServerOptions options;
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

  public AeronServerOptions options() {
    return options;
  }

  public AeronServerSettings options(Consumer<AeronServerOptions.Builder> consumer) {
    return new Builder(this).options(consumer).build();
  }

  public AeronServerSettings options(AeronServerOptions options) {
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
    private AeronServerOptions options;
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

    public Builder options(AeronServerOptions options) {
      this.options = options;
      return this;
    }

    /**
     * Adds aeron server options via its builder consumer.
     *
     * @param options options
     * @return self
     */
    public Builder options(Consumer<AeronServerOptions.Builder> options) {
      AeronServerOptions.Builder optionsBuilder = AeronServerOptions.builder();
      options.accept(optionsBuilder);
      this.options = optionsBuilder.build();
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
