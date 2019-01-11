package reactor.aeron;

import io.aeron.Image;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.DirectBuffer;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

public final class DefaultAeronInbound implements AeronInbound {

  private static final AtomicLongFieldUpdater<DefaultAeronInbound> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(DefaultAeronInbound.class, "requested");

  private static final AtomicReferenceFieldUpdater<DefaultAeronInbound, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              DefaultAeronInbound.class, CoreSubscriber.class, "destinationSubscriber");

  private final Image image;
  private final FragmentHandler fragmentHandler = new FragmentHandler();

  public DefaultAeronInbound(Image image) {
    this.image = image;
  }

  @Override
  public ByteBufferFlux receive() {
    return new ByteBufferFlux(processor.onBackpressureBuffer());
  }

  long produced;

  int poll() {
    long r = requested;
    long p = produced;

    if (p < r) {
      p += image.poll(fragmentHandler, (int) (r - p));
      produced = p;
      // i <= requested
    }
  }

  void dispose() {
    sink.complete();
  }

  private class FragmentHandler implements io.aeron.logbuffer.FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      ByteBuffer dstBuffer = ByteBuffer.allocate(length);
      buffer.getBytes(offset, dstBuffer, length);
      dstBuffer.flip();
      sink.next(dstBuffer);
    }
  }

  private volatile long requested;
  private volatile CoreSubscriber<? super ByteBuffer> destinationSubscriber;

  private class FluxReceive extends Flux<ByteBuffer> implements org.reactivestreams.Subscription {

    @Override
    public void request(long n) {
      Operators.addCap(REQUESTED, DefaultAeronInbound.this, n);
    }

    @Override
    public void cancel() {
      // TODO implement me; research what reactor netty doing in such situatino
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuffer> destinationSubscriber) {
      boolean destinationSet =
          DESTINATION_SUBSCRIBER.compareAndSet(
              DefaultAeronInbound.this, null, destinationSubscriber);
      if (destinationSet) {
        destinationSubscriber.onSubscribe(this);
      } else {
        Operators.error(destinationSubscriber, Exceptions.duplicateOnSubscribeException());
      }
    }
  }
}
