package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

// TODO no clear what to do with this et al
public class DataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

  private volatile Subscription subscription;
  private volatile Subscriber<? super ByteBuffer> subscriber;

  @Override
  public void onSubscription(Subscription subscription) {
    this.subscription = subscription;
  }

  @Override
  public void onNext(ByteBuffer buffer) {
    subscriber.onNext(buffer);
  }

  @Override
  public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(subscription);
  }
}
