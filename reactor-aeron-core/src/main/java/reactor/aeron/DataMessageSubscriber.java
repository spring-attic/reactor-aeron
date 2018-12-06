package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Subscription;

public interface DataMessageSubscriber {

  void onSubscription(Subscription subscription);

  void onNext(long sessionId, ByteBuffer buffer);

  void onComplete(long sessionId);
}
