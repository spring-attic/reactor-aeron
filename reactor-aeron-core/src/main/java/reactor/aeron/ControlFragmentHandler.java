package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import reactor.util.Logger;
import reactor.util.Loggers;

public class ControlFragmentHandler implements FragmentHandler {

  private final Logger logger = Loggers.getLogger(ControlFragmentHandler.class);

  private final ControlMessageSubscriber subscriber;

  public ControlFragmentHandler(ControlMessageSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int index = offset;

    // get MessageType length and MessageType string
    int messageTypeLength = buffer.getInt(index);
    index += BitUtil.SIZE_OF_INT;
    String messageTypeString = buffer.getStringWithoutLengthAscii(index, messageTypeLength);
    index += messageTypeLength;

    MessageType messageType;
    try {
      messageType = MessageType.valueOf(messageTypeString);
    } catch (IllegalArgumentException ex) {
      logger.error("Unknown message type: {}", messageTypeString);
      return;
    }

    if (messageType == MessageType.CONNECT_ACK) {
      long sessionId = buffer.getLong(index);
      index += BitUtil.SIZE_OF_LONG;

      int serverSessionStreamId = buffer.getInt(index);
      index += BitUtil.SIZE_OF_INT;

      long connectRequestId = buffer.getLong(index);

      subscriber.onConnectAck(connectRequestId, sessionId, serverSessionStreamId);
      return;
    }

    if (messageType == MessageType.CONNECT) {
      final long connectRequestId = buffer.getLong(index);
      index += BitUtil.SIZE_OF_LONG;

      int clientChannelLength = buffer.getInt(index);
      index += BitUtil.SIZE_OF_INT;
      String clientChannel = buffer.getStringWithoutLengthAscii(index, clientChannelLength);
      index += clientChannelLength;

      int clientControlStreamId = buffer.getInt(index);
      index += BitUtil.SIZE_OF_INT;

      int clientSessionStreamId = buffer.getInt(index);

      subscriber.onConnect(
          connectRequestId, clientChannel, clientControlStreamId, clientSessionStreamId);
      return;
    }

    if (messageType == MessageType.COMPLETE) {
      long sessionId = buffer.getLong(index);
      subscriber.onComplete(sessionId);
      return;
    }

    // Must be unreachable
    logger.error("Unsupported message type: {}", messageType);
  }
}
