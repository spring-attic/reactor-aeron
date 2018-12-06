package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;

public interface DataMessageSubscriber extends Consumer<Subscription> {

  void onNext(long sessionId, ByteBuffer buffer);

  void onComplete(long sessionId);
}
