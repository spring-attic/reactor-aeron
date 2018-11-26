package reactor.aeron.server;

import reactor.aeron.AeronOptions;

public final class AeronServerOptions extends AeronOptions<AeronServerOptions> {

  public static Builder builder() {
    return new Builder();
  }

  private AeronServerOptions(Builder builder) {
    super(builder);
  }

  public static class Builder extends AeronOptions.Builder<Builder, AeronServerOptions> {

    @Override
    public AeronServerOptions build() {
      return new AeronServerOptions(this);
    }
  }
}
