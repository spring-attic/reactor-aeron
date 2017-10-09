package reactor.ipc.aeron;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public interface DataMessageSubscriber extends PoolerSubscriber {

    void onNext(long sessionId, ByteBuffer buffer);

    void onComplete(long sessionId);

}
