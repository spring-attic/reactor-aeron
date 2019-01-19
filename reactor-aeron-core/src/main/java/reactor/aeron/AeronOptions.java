package reactor.aeron;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
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
  private Function<Object, Integer> bufferCalculator;
  private Function<MutableDirectBuffer, Function<Integer, Function<Object, Void>>> bufferWriter;
  private Function<Object, DirectBuffer> bufferMapper;
  private Consumer<Object> bufferDisposer;

  public AeronOptions() {}

  AeronOptions(AeronOptions other) {
    this.resources = other.resources;
    this.handler = other.handler;
    this.inboundUri = other.inboundUri;
    this.outboundUri = other.outboundUri;
    this.connectTimeout = other.connectTimeout;
    this.backpressureTimeout = other.backpressureTimeout;
    this.adminActionTimeout = other.adminActionTimeout;
    this.bufferCalculator = other.bufferCalculator;
    this.bufferWriter = other.bufferWriter;
    this.bufferMapper = other.bufferMapper;
    this.bufferDisposer = other.bufferDisposer;
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

  public Function<Object, Integer> bufferCalculator() {
    return bufferCalculator;
  }

  public AeronOptions bufferCalculator(Function<Object, Integer> bufferCalculator) {
    return set(s -> s.bufferCalculator = bufferCalculator);
  }

  public Function<MutableDirectBuffer, Function<Integer, Function<Object, Void>>> bufferWriter() {
    return bufferWriter;
  }

  public AeronOptions bufferWriter(
      Function<MutableDirectBuffer, Function<Integer, Function<Object, Void>>> bufferWriter) {
    return set(s -> s.bufferWriter = bufferWriter);
  }

  public Function<Object, DirectBuffer> bufferMapper() {
    return bufferMapper;
  }

  public AeronOptions bufferMapper(Function<Object, DirectBuffer> bufferMapper) {
    return set(s -> s.bufferMapper = bufferMapper);
  }

  public Consumer<Object> bufferDisposer() {
    return bufferDisposer;
  }

  public AeronOptions bufferDisposer(Consumer<Object> bufferDisposer) {
    return set(s -> s.bufferDisposer = bufferDisposer);
  }

  private AeronOptions set(Consumer<AeronOptions> c) {
    AeronOptions s = new AeronOptions(this);
    c.accept(s);
    return s;
  }
}
