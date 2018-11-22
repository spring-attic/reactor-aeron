package reactor.aeron;

import java.nio.ByteBuffer;

public interface DataMessageSubscriber extends PollerSubscriber {

  void onNext(long sessionId, ByteBuffer buffer);

  void onComplete(long sessionId);
}
