package reactor.aeron;

import static java.lang.Boolean.TRUE;

import io.aeron.ChannelUriStringBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class AeronOptions<O extends AeronOptions<O>> {

  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration BACKPRESSURE_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration CONTROL_BACKPRESSURE_TIMEOUT = Duration.ofSeconds(5);
  public static final int CONTROL_STREAM_ID = 1;

  private final ChannelUriStringBuilder serverChannel;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration controlBackpressureTimeout;
  private final int controlStreamId;

  protected AeronOptions(Builder builder) {
    this.serverChannel = builder.serverChannel.validate();
    this.connectTimeout = validate(builder.connectTimeout, "connectTimeout");
    this.backpressureTimeout = validate(builder.backpressureTimeout, "backpressureTimeout");
    this.controlBackpressureTimeout =
        validate(builder.controlBackpressureTimeout, "controlBackpressureTimeout");
    this.controlStreamId = validate(builder.controlStreamId, "controlStreamId");
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
    return controlBackpressureTimeout;
  }

  public int controlStreamId() {
    return controlStreamId;
  }

  protected Duration validate(Duration value, String name) {
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

  public abstract static class Builder<B extends Builder<B, O>, O extends AeronOptions<O>> {

    private ChannelUriStringBuilder serverChannel =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp");
    private Duration connectTimeout = CONNECT_TIMEOUT;
    private Duration backpressureTimeout = BACKPRESSURE_TIMEOUT;
    private Duration controlBackpressureTimeout = CONTROL_BACKPRESSURE_TIMEOUT;
    private Integer controlStreamId = CONTROL_STREAM_ID;

    public B serverChannel(ChannelUriStringBuilder serverChannel) {
      this.serverChannel = serverChannel;
      return self();
    }

    public B serverChannel(Consumer<ChannelUriStringBuilder> serverChannel) {
      serverChannel.accept(this.serverChannel);
      return self();
    }

    public B connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return self();
    }

    public B backpressureTimeout(Duration backpressureTimeout) {
      this.backpressureTimeout = backpressureTimeout;
      return self();
    }

    public B controlBackpressureTimeout(Duration controlBackpressureTimeout) {
      this.controlBackpressureTimeout = controlBackpressureTimeout;
      return self();
    }

    public B controlStreamId(int controlStreamId) {
      this.controlStreamId = controlStreamId;
      return self();
    }

    public abstract O build();

    private B self() {
      //noinspection unchecked
      return (B) this;
    }
  }
}
