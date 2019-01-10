package reactor.aeron;

import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;

// TODO think of better design for inbound -- dont allow clients cast to FragmentHandler or
// Disposable
public final class DefaultAeronInbound implements AeronInbound, FragmentHandler, Disposable {

  private final EmitterProcessor<ByteBuffer> processor = EmitterProcessor.create();
  private final FluxSink<ByteBuffer> sink = processor.sink();

  private final Image image;

  public DefaultAeronInbound(Image image) {
    this.image = image;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
    buffer.getBytes(offset, dstBuffer, length);
    dstBuffer.flip();
    sink.next(dstBuffer);

    // TODO sink.next may lead to forever-spin effect -- EmitterProcessor.java:262; current model is
    // too greedy -- i.e. dont care about presence of any Subscriber et al, it's just read read
    // read, and then comes EmitterProcessor.java:262
    // This problem very much coupled with problem at MessageSubscription.poll()

    /*
       java.lang.Thread.State: TIMED_WAITING
    at sun.misc.Unsafe.park(Native Method)
    at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:338)
    at reactor.core.publisher.EmitterProcessor.onNext(EmitterProcessor.java:262)
    at reactor.core.publisher.FluxCreate$IgnoreSink.next(FluxCreate.java:593)
    at reactor.core.publisher.FluxCreate$SerializedSink.next(FluxCreate.java:151)
    at reactor.aeron.DefaultAeronInbound.onFragment(DefaultAeronInbound.java:23)
    at io.aeron.FragmentAssembler.onFragment(FragmentAssembler.java:118)
    at io.aeron.logbuffer.TermReader.read(TermReader.java:80)
    at io.aeron.Image.poll(Image.java:285)
    at io.aeron.Subscription.poll(Subscription.java:204)
    at reactor.aeron.MessageSubscription.poll(MessageSubscription.java:53)
    at reactor.aeron.AeronEventLoop$Worker.run(AeronEventLoop.java:291)
    at java.lang.Thread.run(Thread.java:748)
     */
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
