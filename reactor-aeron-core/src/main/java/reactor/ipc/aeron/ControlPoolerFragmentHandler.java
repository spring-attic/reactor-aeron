package reactor.ipc.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.UUID;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import reactor.util.Logger;
import reactor.util.Loggers;

public class ControlPoolerFragmentHandler implements FragmentHandler {

  private final Logger logger = Loggers.getLogger(ControlPoolerFragmentHandler.class);

  private final ControlMessageSubscriber subscriber;

  public ControlPoolerFragmentHandler(ControlMessageSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int index = offset;
    int type = buffer.getByte(index);
    index += BitUtil.SIZE_OF_BYTE;
    long sessionId = buffer.getLong(index);
    index += BitUtil.SIZE_OF_LONG;

    if (type == MessageType.CONNECT.ordinal()) {
      long mostSigBits = buffer.getLong(index);
      index += BitUtil.SIZE_OF_LONG;
      long leastSigBits = buffer.getLong(index);
      index += BitUtil.SIZE_OF_LONG;
      UUID connectRequestId = new UUID(mostSigBits, leastSigBits);

      int channelLength = buffer.getInt(index);
      String channel = buffer.getStringUtf8(index, channelLength);
      index += BitUtil.SIZE_OF_INT + channelLength;

      int clientControlStreamId = buffer.getInt(index);
      index += BitUtil.SIZE_OF_INT;

      int clientSessionStreamId = buffer.getInt(index);

      subscriber.onConnect(connectRequestId, channel, clientControlStreamId, clientSessionStreamId);
    } else if (type == MessageType.CONNECT_ACK.ordinal()) {
      int serverSessionStreamId = buffer.getInt(index);
      index += BitUtil.SIZE_OF_INT;

      long mostSigBits = buffer.getLong(index);
      index += BitUtil.SIZE_OF_LONG;
      long leastSigBits = buffer.getLong(index);
      UUID connectRequestId = new UUID(mostSigBits, leastSigBits);

      subscriber.onConnectAck(connectRequestId, sessionId, serverSessionStreamId);
    } else if (type == MessageType.HEARTBEAT.ordinal()) {
      subscriber.onHeartbeat(sessionId);
    } else if (type == MessageType.COMPLETE.ordinal()) {
      subscriber.onComplete(sessionId);
    } else {
      // TODO: Add publication channel into the message
      logger.error("Unknown message type id: {}", type);
    }
  }
}
