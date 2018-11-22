package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public final class ByteBufferFlux extends Flux<ByteBuffer> {

  private final Publisher<ByteBuffer> source;

  public static ByteBufferFlux from(String... data) {
    return new ByteBufferFlux(Flux.fromArray(data).map(AeronUtils::stringToByteBuffer));
  }

  public ByteBufferFlux(Publisher<ByteBuffer> source) {
    this.source = source;
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuffer> s) {
    source.subscribe(s);
  }

  public Flux<String> asString() {
    return map(AeronUtils::byteBufferToString);
  }
}
