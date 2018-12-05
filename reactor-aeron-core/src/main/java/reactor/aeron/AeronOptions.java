package reactor.aeron;

import io.aeron.ChannelUriStringBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class AeronOptions {

  public static final Duration ACK_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration BACKPRESSURE_TIMEOUT = Duration.ofSeconds(5);

  private final ChannelUriStringBuilder serverChannel;
  private final ChannelUriStringBuilder clientChannel;
  private final Duration ackTimeout;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;

  private AeronOptions(Builder builder) {
    this.serverChannel = builder.serverChannel.validate();
    this.clientChannel = builder.clientChannel.validate();
    this.ackTimeout = validate(builder.ackTimeout, "ackTimeout");
    this.connectTimeout = validate(builder.connectTimeout, "connectTimeout");
    this.backpressureTimeout = validate(builder.backpressureTimeout, "backpressureTimeout");
  }

  public static Builder builder() {
    return new Builder();
  }

  public String serverChannel() {
    return serverChannel.build();
  }

  public String clientChannel() {
    return clientChannel.build();
  }

  public Duration ackTimeout() {
    return ackTimeout;
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public Duration backpressureTimeout() {
    return backpressureTimeout;
  }

  private Duration validate(Duration value, String message) {
    Objects.requireNonNull(value, message);
    if (value.compareTo(Duration.ZERO) <= 0) {
      throw new IllegalArgumentException(message + " > 0 expected, but got: " + value);
    }
    return value;
  }

  private int validate(Integer value, String message) {
    Objects.requireNonNull(value, message);
    if (value <= 0) {
      throw new IllegalArgumentException(message + " > 0 expected, but got: " + value);
    }
    return value;
  }

  public static class Builder {

    private ChannelUriStringBuilder serverChannel = serverChannelBuilder();
    private ChannelUriStringBuilder clientChannel = clientChannelBuilder();
    private Duration ackTimeout = ACK_TIMEOUT;
    private Duration connectTimeout = CONNECT_TIMEOUT;
    private Duration backpressureTimeout = BACKPRESSURE_TIMEOUT;

    private Builder() {}

    public Builder serverChannel(ChannelUriStringBuilder serverChannel) {
      this.serverChannel = serverChannel;
      return this;
    }

    /**
     * Sets server uri via consumer.
     *
     * @param consumer consumer
     * @return this builder
     */
    public Builder serverChannel(Consumer<ChannelUriStringBuilder> consumer) {
      ChannelUriStringBuilder channelBuilder = serverChannelBuilder();
      consumer.accept(channelBuilder);
      this.serverChannel = channelBuilder;
      return this;
    }

    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder backpressureTimeout(Duration backpressureTimeout) {
      this.backpressureTimeout = backpressureTimeout;
      return this;
    }

    public Builder clientChannel(ChannelUriStringBuilder clientChannel) {
      this.clientChannel = clientChannel;
      return this;
    }

    /**
     * Sets client uri via consumer.
     *
     * @param consumer consumer
     * @return this builder
     */
    public Builder clientChannel(Consumer<ChannelUriStringBuilder> consumer) {
      ChannelUriStringBuilder builder = clientChannelBuilder();
      consumer.accept(builder);
      this.clientChannel = builder;
      return this;
    }

    public Builder ackTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    private ChannelUriStringBuilder serverChannelBuilder() {
      return new ChannelUriStringBuilder().reliable(true).media("udp");
    }

    private ChannelUriStringBuilder clientChannelBuilder() {
      return new ChannelUriStringBuilder().reliable(true).media("udp").endpoint("localhost:0");
    }

    public AeronOptions build() {
      return new AeronOptions(this);
    }
  }
}
