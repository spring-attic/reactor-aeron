package reactor.ipc.aeron;

import java.nio.ByteBuffer;

public interface DataMessageSubscriber extends PoolerSubscriber {

  void onNext(long sessionId, ByteBuffer buffer);

  void onComplete(long sessionId);
}
