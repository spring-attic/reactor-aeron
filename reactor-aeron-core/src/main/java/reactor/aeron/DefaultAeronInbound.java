package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;

public final class DefaultAeronInbound implements AeronInbound, FragmentHandler, Disposable {

  private final EmitterProcessor<ByteBuffer> processor = EmitterProcessor.create();
  private final FluxSink<ByteBuffer> sink = processor.sink();

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
    sink.complete();
  }

  @Override
  public boolean isDisposed() {
    return processor.isDisposed();
  }
}
