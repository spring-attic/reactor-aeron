package reactor.aeron.client;

import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.OnDisposable;

public class AeronClientSettings {

  private final String name;
  private final AeronResources aeronResources;
  private final AeronOptions options;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  private AeronClientSettings(Builder builder) {
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

  public Function<? super Connection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronClientSettings handler(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new Builder(this).handler(handler).build();
  }

  public AeronOptions options() {
    return options;
  }

  public AeronClientSettings options(Consumer<AeronOptions.Builder> consumer) {
    return new Builder(this).options(consumer).build();
  }

  public AeronClientSettings options(AeronOptions options) {
    return new Builder(this).options(options).build();
  }

  public static class Builder {

    private String name;
    private AeronResources aeronResources;
    private AeronOptions options;
    private Function<? super Connection, ? extends Publisher<Void>> handler =
        OnDisposable::onDispose;

    private Builder() {}

    private Builder(AeronClientSettings settings) {
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

    /**
     * Adds aeron client options via its builder consumer.
     *
     * @param options options
     * @return self
     */
    public Builder options(Consumer<AeronOptions.Builder> options) {
      AeronOptions.Builder optionsBuilder = AeronOptions.builder();
      options.accept(optionsBuilder);
      this.options = optionsBuilder.build();
      return this;
    }

    public Builder handler(Function<? super Connection, ? extends Publisher<Void>> handler) {
      this.handler = handler;
      return this;
    }

    public AeronClientSettings build() {
      return new AeronClientSettings(this);
    }
  }
}
