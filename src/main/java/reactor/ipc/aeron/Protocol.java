package reactor.ipc.aeron;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

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

    public static ByteBuffer createConnectBody(String clientChannel, int clientStreamId) {
        byte[] clientChannelBytes = clientChannel.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[clientChannelBytes.length + BitUtil.SIZE_OF_INT * 2];
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        int index = 0;
        buffer.putInt(index, clientChannelBytes.length);
        index += BitUtil.SIZE_OF_INT;
        buffer.putBytes(index, clientChannelBytes);
        index += clientChannelBytes.length;
        buffer.putInt(index, clientStreamId);
        index += BitUtil.SIZE_OF_INT;
        return ByteBuffer.wrap(bytes, 0, index);
    }

}
