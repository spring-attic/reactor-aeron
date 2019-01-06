package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public final class DefaultAeronInbound implements AeronInbound, FragmentHandler, OnDisposable {

  private final EmitterProcessor<ByteBuffer> processor = EmitterProcessor.create();
  private final FluxSink<ByteBuffer> sink = processor.sink();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  public DefaultAeronInbound() {
    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
    buffer.getBytes(offset, dstBuffer, length);
    dstBuffer.flip();
    sink.next(dstBuffer);
  }

  @Override
  public ByteBufferFlux receive() {
    return new ByteBufferFlux(processor.onBackpressureBuffer());
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.fromRunnable(sink::complete);
  }
}
