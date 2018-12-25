package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

/** Protocol. */
public class Protocol {

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
    int connectLength = MessageType.CONNECT.toString().getBytes(StandardCharsets.US_ASCII).length;
    int clientChannelLength = clientChannel.getBytes(StandardCharsets.US_ASCII).length;
    int bytesLength =
        BitUtil.SIZE_OF_INT
            + connectLength /*MessageType.CONNECT*/
            + BitUtil.SIZE_OF_INT
            + clientChannelLength /*clientChannel*/
            + BitUtil.SIZE_OF_INT /*clientControlStreamId*/
            + BitUtil.SIZE_OF_INT /*clientSessionStreamId*/
            + BitUtil.SIZE_OF_LONG /*connectRequestId*/;

    byte[] bytes = new byte[bytesLength];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;

    // put MessageType.CONNECT length
    buffer.putInt(index, connectLength);
    index += BitUtil.SIZE_OF_INT;
    // put MessageType.CONNECT
    buffer.putStringWithoutLengthAscii(index, MessageType.CONNECT.toString());
    index += connectLength;

    // put connectRequestId
    buffer.putLong(index, connectRequestId);
    index += BitUtil.SIZE_OF_LONG;

    // put clientChannel length
    buffer.putInt(index, clientChannelLength);
    index += BitUtil.SIZE_OF_INT;
    buffer.putStringWithoutLengthAscii(index, clientChannel);
    index += clientChannelLength;

    // put clientControlStreamId
    buffer.putInt(index, clientControlStreamId);
    index += BitUtil.SIZE_OF_INT;

    // put clientSessionStreamId
    buffer.putInt(index, clientSessionStreamId);
    index += BitUtil.SIZE_OF_INT;

    return ByteBuffer.wrap(bytes, 0, index);
  }

  /**
   * Factory for connect ack body.
   *
   * @param sessionId session id
   * @param connectRequestId connect request id
   * @param serverSessionStreamId server session stream id
   * @return bytebuffer of connect ack body
   */
  public static ByteBuffer createConnectAckBody(
      long sessionId, long connectRequestId, int serverSessionStreamId) {
    int connectAckLength =
        MessageType.CONNECT_ACK.toString().getBytes(StandardCharsets.US_ASCII).length;
    int bytesLength =
        BitUtil.SIZE_OF_INT
            + connectAckLength /*MessageType.CONNECT_ACK*/
            + BitUtil.SIZE_OF_LONG /*sessionId*/
            + BitUtil.SIZE_OF_INT /*serverSessionStreamId*/
            + BitUtil.SIZE_OF_LONG /*connectRequestId*/;

    byte[] bytes = new byte[bytesLength];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;

    // put MessageType.CONNECT_ACK length
    buffer.putInt(index, connectAckLength);
    index += BitUtil.SIZE_OF_INT;
    // put MessageType.CONNECT_ACK
    buffer.putStringWithoutLengthAscii(index, MessageType.CONNECT_ACK.toString());
    index += connectAckLength;

    // put sessionId
    buffer.putLong(index, sessionId);
    index += BitUtil.SIZE_OF_LONG;

    // put serverSessionStreamId
    buffer.putInt(index, serverSessionStreamId);
    index += BitUtil.SIZE_OF_INT;

    // put connectRequestId
    buffer.putLong(index, connectRequestId);
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Factory for disconnect body bytebuffer.
   *
   * @param sessionId session id
   * @return bytebuffer of disconnect body
   */
  public static ByteBuffer createDisconnectBody(long sessionId) {
    int disconnectLength = MessageType.COMPLETE.toString().getBytes().length;
    int bytesLength =
        BitUtil.SIZE_OF_INT
            + disconnectLength /*MessageType.COMPLETE*/
            + BitUtil.SIZE_OF_LONG /*sessionId*/;

    byte[] bytes = new byte[bytesLength];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;

    // put MessageType.COMPLETE length
    buffer.putInt(index, disconnectLength);
    index += BitUtil.SIZE_OF_INT;
    // put MessageType.COMPLETE
    buffer.putStringWithoutLengthAscii(index, MessageType.COMPLETE.toString());
    index += disconnectLength;

    // put sessionId
    buffer.putLong(index, sessionId);
    return ByteBuffer.wrap(bytes);
  }
}
