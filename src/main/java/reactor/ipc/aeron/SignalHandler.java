package reactor.ipc.aeron;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public interface SignalHandler {

    void onConnect(UUID sessionId, String channel, int streamId);

    void onNext(UUID sessionId, ByteBuffer buffer);

}
