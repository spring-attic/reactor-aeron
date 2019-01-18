package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public final class ByteBufferFlux extends FluxOperator<ByteBuffer, ByteBuffer> {

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
}
