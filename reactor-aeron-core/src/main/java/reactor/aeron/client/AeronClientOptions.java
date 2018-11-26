package reactor.aeron.client;

import static java.lang.Boolean.TRUE;

import io.aeron.ChannelUriStringBuilder;
import java.time.Duration;
import java.util.function.Consumer;
import reactor.aeron.AeronOptions;

public final class AeronClientOptions extends AeronOptions<AeronClientOptions> {

  public static final Duration ACK_TIMEOUT = Duration.ofSeconds(10);

  private final ChannelUriStringBuilder clientChannel;
  private final Duration ackTimeout;

  public static Builder builder() {
    return new Builder();
  }

  private AeronClientOptions(Builder builder) {
    super(builder);
    this.clientChannel = builder.clientChannel.validate();
    this.ackTimeout = validate(builder.ackTimeout, "ackTimeout");
  }

  public String clientChannel() {
    return clientChannel.build();
  }

  public Duration ackTimeout() {
    return ackTimeout;
  }

  public static class Builder extends AeronOptions.Builder<Builder, AeronClientOptions> {

    private ChannelUriStringBuilder clientChannel =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp").endpoint("localhost:" + 0);
    private Duration ackTimeout = ACK_TIMEOUT;

    public Builder clientChannel(ChannelUriStringBuilder clientChannel) {
      this.clientChannel = clientChannel;
      return this;
    }

    public Builder clientChannel(Consumer<ChannelUriStringBuilder> clientChannel) {
      clientChannel.accept(this.clientChannel);
      return this;
    }

    public Builder ackTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    public AeronClientOptions build() {
      return new AeronClientOptions(this);
    }
  }
}
