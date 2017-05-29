package reactor.ipc.aeron;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Protocol {

    public static final int HEADER_SIZE = BitUtil.SIZE_OF_BYTE + BitUtil.SIZE_OF_LONG * 2;

    static int putHeader(MutableDirectBuffer buffer, int index, MessageType requestType, UUID sessionId) {
        buffer.putByte(index, (byte) requestType.ordinal());
        index += BitUtil.SIZE_OF_BYTE;
        buffer.putLong(index, sessionId.getMostSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        buffer.putLong(index, sessionId.getLeastSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        return index;
    }

    public static ByteBuffer createConnectBody(String clientChannel, int clientStreamId, int clientAckStreamId) {
        byte[] clientChannelBytes = clientChannel.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[clientChannelBytes.length + BitUtil.SIZE_OF_INT * 3];
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        int index = 0;
        buffer.putInt(index, clientChannelBytes.length);
        index += BitUtil.SIZE_OF_INT;
        buffer.putBytes(index, clientChannelBytes);
        index += clientChannelBytes.length;
        buffer.putInt(index, clientStreamId);
        index += BitUtil.SIZE_OF_INT;
        buffer.putInt(index, clientAckStreamId);
        index += BitUtil.SIZE_OF_INT;
        return ByteBuffer.wrap(bytes, 0, index);
    }

    public static ByteBuffer createConnectAckBody(int streamId) {
        byte array[] = new byte[BitUtil.SIZE_OF_INT];
        UnsafeBuffer buffer = new UnsafeBuffer(array);
        buffer.putInt(0, streamId);
        return ByteBuffer.wrap(array);
    }
}
