package reactor.aeron;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

/**
 * Immutable wrapper around options for full-duplex aeron <i>connection</i> between client and
 * server. Note, it's mandatory to set {@code resources}, {@code inboundUri} and {@code
 * outboundUri}, everything rest may come with defaults.
 */
public final class AeronOptions {

  private AeronResources resources;
  private Function<? super AeronConnection, ? extends Publisher<Void>> handler;
  private AeronChannelUriString inboundUri = new AeronChannelUriString();
  private AeronChannelUriString outboundUri = new AeronChannelUriString();
  private Duration connectTimeout = Duration.ofSeconds(5);
  private Duration backpressureTimeout = Duration.ofSeconds(5);
  private Duration adminActionTimeout = Duration.ofSeconds(5);
  private Supplier<Integer> sessionIdGenerator;

  public AeronOptions() {}

  AeronOptions(AeronOptions other) {
    this.resources = other.resources;
    this.handler = other.handler;
    this.inboundUri = other.inboundUri;
    this.outboundUri = other.outboundUri;
    this.connectTimeout = other.connectTimeout;
    this.backpressureTimeout = other.backpressureTimeout;
    this.adminActionTimeout = other.adminActionTimeout;
    this.sessionIdGenerator = other.sessionIdGenerator;
  }

  public AeronResources resources() {
    return resources;
  }

  public AeronOptions resources(AeronResources resources) {
    return set(s -> s.resources = resources);
  }

  public Function<? super AeronConnection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronOptions handler(
      Function<? super AeronConnection, ? extends Publisher<Void>> handler) {
    return set(s -> s.handler = handler);
  }

  public AeronChannelUriString inboundUri() {
    return inboundUri;
  }

  public AeronOptions inboundUri(AeronChannelUriString inboundUri) {
    return set(s -> s.inboundUri = inboundUri);
  }

  public AeronChannelUriString outboundUri() {
    return outboundUri;
  }

  public AeronOptions outboundUri(AeronChannelUriString outboundUri) {
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

  Supplier<Integer> sessionIdGenerator() {
    return sessionIdGenerator;
  }

  AeronOptions sessionIdGenerator(Supplier<Integer> sessionIdGenerator) {
    return set(s -> s.sessionIdGenerator = sessionIdGenerator);
  }

  private AeronOptions set(Consumer<AeronOptions> c) {
    AeronOptions s = new AeronOptions(this);
    c.accept(s);
    return s;
  }
}
