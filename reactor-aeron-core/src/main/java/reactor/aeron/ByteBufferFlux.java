package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public final class ByteBufferFlux extends FluxOperator<ByteBuffer, ByteBuffer> {

  public ByteBufferFlux(Publisher<? extends ByteBuffer> source) {
    this(Flux.from(source));
  }

  public ByteBufferFlux(Flux<? extends ByteBuffer> source) {
    super(source);
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuffer> s) {
    source.subscribe(s);
  }

  public Flux<String> asString() {
    return map(buffer -> StandardCharsets.UTF_8.decode(buffer).toString());
  }

  public static ByteBufferFlux fromString(String... data) {
    return new ByteBufferFlux(
        Flux.fromArray(data).map(s -> s.getBytes(StandardCharsets.UTF_8)).map(ByteBuffer::wrap));
  }

  public static ByteBufferFlux fromString(Publisher<String> source) {
    return new ByteBufferFlux(
        Flux.from(source).map(s -> s.getBytes(StandardCharsets.UTF_8)).map(ByteBuffer::wrap));
  }
}
