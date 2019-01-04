package reactor.aeron;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;

public final class AeronOptions {

  private AeronResources resources;
  private Function<? super Connection, ? extends Publisher<Void>> handler;
  private AeronChannelUri inboundUri = new AeronChannelUri();
  private AeronChannelUri outboundUri = new AeronChannelUri();
  private Duration connectTimeout = Duration.ofSeconds(5);
  private Duration backpressureTimeout = Duration.ofSeconds(5);

  public AeronOptions() {}

  public AeronOptions(AeronOptions other) {
    this.resources = other.resources;
    this.handler = other.handler;
    this.inboundUri = other.inboundUri;
    this.outboundUri = other.outboundUri;
    this.connectTimeout = other.connectTimeout;
    this.backpressureTimeout = other.backpressureTimeout;
  }

  public AeronResources resources() {
    return resources;
  }

  public Function<? super Connection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronChannelUri inboundUri() {
    return inboundUri;
  }

  public AeronChannelUri outboundUri() {
    return outboundUri;
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public Duration backpressureTimeout() {
    return backpressureTimeout;
  }

  public AeronOptions resources(AeronResources resources) {
    return set(s -> s.resources = resources);
  }

  public AeronOptions handler(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return set(s -> s.handler = handler);
  }

  public void inboundUri(AeronChannelUri inboundUri) {
    set(s -> s.inboundUri = inboundUri);
  }

  public AeronOptions outboundUri(AeronChannelUri outboundUri) {
    return set(s -> s.outboundUri = outboundUri);
  }

  public AeronOptions connectTimeout(Duration connectTimeout) {
    return set(s -> s.connectTimeout = connectTimeout);
  }

  public AeronOptions backpressureTimeout(Duration backpressureTimeout) {
    return set(s -> s.backpressureTimeout = backpressureTimeout);
  }

  private AeronOptions set(Consumer<AeronOptions> c) {
    AeronOptions s = new AeronOptions(this);
    c.accept(s);
    return s;
  }
}
