package reactor.aeron;

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
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
  private final io.aeron.logbuffer.FragmentHandler fragmentHandler =
      new ImageFragmentAssembler(new FragmentHandler());
  private final FluxReceive inbound = new FluxReceive();

  private volatile long requested;
  private volatile CoreSubscriber<? super ByteBuffer> destinationSubscriber;

  public DefaultAeronInbound(Image image) {
    this.image = image;
  }

  @Override
  public ByteBufferFlux receive() {
    // todo do we need to use onBackpressureBuffer here?
    return new ByteBufferFlux(inbound.onBackpressureBuffer());
  }

  int poll() {
    int r = (int) Math.min(requested, 8);
    int produced = 0;
    if (r > 0) {
      produced = image.poll(fragmentHandler, r);
      if (produced > 0) {
        Operators.produced(REQUESTED, this, produced);
      }
    }
    return produced;
  }

  void dispose() {
    inbound.cancel();
  }

  private class FragmentHandler implements io.aeron.logbuffer.FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      ByteBuffer dstBuffer = ByteBuffer.allocate(length);
      buffer.getBytes(offset, dstBuffer, length);
      dstBuffer.flip();

      CoreSubscriber<? super ByteBuffer> destination =
          DefaultAeronInbound.this.destinationSubscriber;
      // check on cancel?
      if (destination != null) {
        destination.onNext(dstBuffer);
      } else {
        // todo need to research io.aeron.ImageControlledFragmentAssembler
        throw new RuntimeException("we have received message without any subscriber");
      }
    }
  }

  private class FluxReceive extends Flux<ByteBuffer> implements org.reactivestreams.Subscription {

    @Override
    public void request(long n) {
      Operators.addCap(REQUESTED, DefaultAeronInbound.this, n);
    }

    @Override
    public void cancel() {
      // TODO implement me; research what reactor netty doing in such situatino
      REQUESTED.set(DefaultAeronInbound.this, 0);
      CoreSubscriber destination = DESTINATION_SUBSCRIBER.getAndSet(DefaultAeronInbound.this, null);
      if (destination != null) {
        destination.onComplete();
      }
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
