package reactor.ipc.aeron;

import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public class PoolerFragmentHandler implements FragmentHandler {

    private Logger logger = Loggers.getLogger(PoolerFragmentHandler.class);

    private final SignalHandler handler;

    public PoolerFragmentHandler(SignalHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        int index = offset;
        int type = buffer.getByte(index);
        index += BitUtil.SIZE_OF_BYTE;
        long sessionIdMostSigBits = buffer.getLong(index);
        index += BitUtil.SIZE_OF_LONG;
        long sessionIdLeastSigBits = buffer.getLong(index);
        index += BitUtil.SIZE_OF_LONG;
        UUID sessionId = new UUID(sessionIdMostSigBits, sessionIdLeastSigBits);

        if (type == RequestType.CONNECT.ordinal()) {
            int channelLength = buffer.getInt(index);
            String channel = buffer.getStringUtf8(index, channelLength);
            index += BitUtil.SIZE_OF_INT + channelLength;
            int streamId = buffer.getInt(index);

            handler.onConnect(sessionId, channel, streamId);
        } else if (type == RequestType.NEXT.ordinal()) {
            int bytesLength = length - (index - offset);
            ByteBuffer dst = ByteBuffer.allocate(bytesLength);
            buffer.getBytes(index, dst, bytesLength);
            dst.rewind();
            
            handler.onNext(sessionId, dst);
        } else {
            logger.error("Unknown type: {}", type);
        }
    }

}
