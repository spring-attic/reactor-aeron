package reactor.aeron.server;

import static java.lang.Boolean.TRUE;

import io.aeron.ChannelUriStringBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class AeronServerOptions {

  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration BACKPRESSURE_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration CONTROL_BACKPRESSURE_TIMEOUT = Duration.ofSeconds(5);
  public static final int SERVER_STREAM_ID = 1;

  private final ChannelUriStringBuilder serverChannel;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration controlBackpressureTimeout;
  private final int serverStreamId;

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(AeronServerOptions options) {
    Builder builder = new Builder();
    builder.serverChannel = options.serverChannel;
    builder.connectTimeout = options.connectTimeout;
    builder.backpressureTimeout = options.backpressureTimeout;
    builder.controlBackpressureTimeout = options.controlBackpressureTimeout;
    builder.serverStreamId = options.serverStreamId;
    return builder;
  }

  private AeronServerOptions(Builder builder) {
    this.serverChannel = builder.serverChannel.validate();
    this.connectTimeout = validate(builder.connectTimeout, "connectTimeout");
    this.backpressureTimeout = validate(builder.backpressureTimeout, "backpressureTimeout");
    this.controlBackpressureTimeout =
        validate(builder.controlBackpressureTimeout, "controlBackpressureTimeout");
    this.serverStreamId = validate(builder.serverStreamId, "serverStreamId");
  }

  public String serverChannel() {
    return serverChannel.build();
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public Duration backpressureTimeout() {
    return backpressureTimeout;
  }

  public Duration controlBackpressureTimeout() {
    return backpressureTimeout;
  }

  public int serverStreamId() {
    return serverStreamId;
  }

  private Duration validate(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.compareTo(Duration.ZERO) <= 0) {
      throw new IllegalArgumentException(name + " > 0 expected, but got: " + name);
    }
    return value;
  }

  private int validate(Integer value, String name) {
    Objects.requireNonNull(value, name);
    if (value <= 0) {
      throw new IllegalArgumentException(name + " > 0 expected, but got: " + name);
    }
    return value;
  }

  public static class Builder {

    private ChannelUriStringBuilder serverChannel =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp");
    private Duration connectTimeout = CONNECT_TIMEOUT;
    private Duration backpressureTimeout = BACKPRESSURE_TIMEOUT;
    private Duration controlBackpressureTimeout = CONTROL_BACKPRESSURE_TIMEOUT;
    private Integer serverStreamId = SERVER_STREAM_ID;

    public Builder serverChannel(ChannelUriStringBuilder serverChannel) {
      this.serverChannel = serverChannel;
      return this;
    }

    public Builder serverChannel(Consumer<ChannelUriStringBuilder> serverChannel) {
      serverChannel.accept(this.serverChannel);
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

    public Builder controlBackpressureTimeout(Duration controlBackpressureTimeout) {
      this.controlBackpressureTimeout = controlBackpressureTimeout;
      return this;
    }

    public Builder serverStreamId(int serverStreamId) {
      this.serverStreamId = serverStreamId;
      return this;
    }

    public AeronServerOptions build() {
      return new AeronServerOptions(this);
    }
  }
}
