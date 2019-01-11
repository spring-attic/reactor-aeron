package reactor.aeron;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;

/**
 * Immutable wrapper around options for full-duplex aeron <i>connection</i> between client and
 * server. Note, it's mandatory to set {@code resources}, {@code inboundUri} and {@code
 * outboundUri}, everything rest may come with defaults.
 */
public final class AeronOptions {

  private AeronResources resources;
  private Function<? super Connection, ? extends Publisher<Void>> handler;
  private AeronChannelUri inboundUri = new AeronChannelUri();
  private AeronChannelUri outboundUri = new AeronChannelUri();
  private Duration connectTimeout = Duration.ofSeconds(5);
  private Duration backpressureTimeout = Duration.ofSeconds(5);
  private Duration adminActionTimeout = Duration.ofSeconds(5);

  public AeronOptions() {}

  /**
   * Copy constructor.
   *
   * @param other instance to copy from
   */
  public AeronOptions(AeronOptions other) {
    this.resources = other.resources;
    this.handler = other.handler;
    this.inboundUri = other.inboundUri;
    this.outboundUri = other.outboundUri;
    this.connectTimeout = other.connectTimeout;
    this.backpressureTimeout = other.backpressureTimeout;
    this.adminActionTimeout = other.adminActionTimeout;
  }

  public AeronResources resources() {
    return resources;
  }

  public AeronOptions resources(AeronResources resources) {
    return set(s -> s.resources = resources);
  }

  public Function<? super Connection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronOptions handler(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return set(s -> s.handler = handler);
  }

  public AeronChannelUri inboundUri() {
    return inboundUri;
  }

  public AeronOptions inboundUri(AeronChannelUri inboundUri) {
    return set(s -> s.inboundUri = inboundUri);
  }

  public AeronChannelUri outboundUri() {
    return outboundUri;
  }

  public AeronOptions outboundUri(AeronChannelUri outboundUri) {
    return set(s -> s.outboundUri = outboundUri);
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public AeronOptions connectTimeout(Duration connectTimeout) {
    return set(s -> s.connectTimeout = connectTimeout);
  }

  public Duration backpressureTimeout() {
    return backpressureTimeout;
  }

  public AeronOptions backpressureTimeout(Duration backpressureTimeout) {
    return set(s -> s.backpressureTimeout = backpressureTimeout);
  }

  public Duration adminActionTimeout() {
    return adminActionTimeout;
  }

  public AeronOptions adminActionTimeout(Duration adminActionTimeout) {
    return set(s -> s.adminActionTimeout = adminActionTimeout);
  }

  private AeronOptions set(Consumer<AeronOptions> c) {
    AeronOptions s = new AeronOptions(this);
    c.accept(s);
    return s;
  }
}
