package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Protocol. */
public class Protocol {

  public static final int HEADER_SIZE = BitUtil.SIZE_OF_BYTE + BitUtil.SIZE_OF_LONG;

  static int putHeader(
      MutableDirectBuffer buffer, int index, MessageType requestType, long sessionId) {
    buffer.putByte(index, (byte) requestType.ordinal());
    index += BitUtil.SIZE_OF_BYTE;

    buffer.putLong(index, sessionId);
    index += BitUtil.SIZE_OF_LONG;
    return index;
  }

  /**
   * Factory method for connect body.
   *
   * @param connectRequestId connect request id
   * @param clientChannel client channel
   * @param clientControlStreamId client control stream id
   * @param clientSessionStreamId client session stream id
   * @return byte buffer
   */
  public static ByteBuffer createConnectBody(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {
    byte[] clientChannelBytes = clientChannel.getBytes(StandardCharsets.UTF_8);
    byte[] bytes =
        new byte[clientChannelBytes.length + BitUtil.SIZE_OF_INT * 3 + BitUtil.SIZE_OF_LONG];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;
    buffer.putLong(index, connectRequestId);
    index += BitUtil.SIZE_OF_LONG;

    buffer.putInt(index, clientChannelBytes.length);
    index += BitUtil.SIZE_OF_INT;
    buffer.putBytes(index, clientChannelBytes);
    index += clientChannelBytes.length;

    buffer.putInt(index, clientControlStreamId);
    index += BitUtil.SIZE_OF_INT;

    buffer.putInt(index, clientSessionStreamId);
    index += BitUtil.SIZE_OF_INT;

    return ByteBuffer.wrap(bytes, 0, index);
  }

  /**
   * Factory for connect ack body.
   *
   * @param connectRequestId connect request id
   * @param serverSessionStreamId server session stream id
   * @return bytebuffer of connect ack body
   */
  public static ByteBuffer createConnectAckBody(long connectRequestId, int serverSessionStreamId) {
    byte[] array = new byte[BitUtil.SIZE_OF_INT + BitUtil.SIZE_OF_LONG];
    UnsafeBuffer buffer = new UnsafeBuffer(array);
    int index = 0;
    buffer.putInt(index, serverSessionStreamId);
    index += BitUtil.SIZE_OF_INT;
    buffer.putLong(index, connectRequestId);
    //noinspection UnusedAssignment
    index += BitUtil.SIZE_OF_LONG;
    return ByteBuffer.wrap(array);
  }

  /**
   * Factory for disconnect body bytebuffer.
   *
   * @param sessionId session id
   * @return bytebuffer of disconnect body
   */
  public static ByteBuffer createDisconnectBody(long sessionId) {
    byte[] bytes = new byte[BitUtil.SIZE_OF_LONG];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    buffer.putLong(0, sessionId);
    return ByteBuffer.wrap(bytes);
  }
}
