package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

// TODO do we need this if we have DefaultAeronInbound.receive()
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
    return map(AeronUtils::byteBufferToString);
  }

  public static ByteBufferFlux from(String... data) {
    return new ByteBufferFlux(Flux.fromArray(data).map(AeronUtils::stringToByteBuffer));
  }
}
