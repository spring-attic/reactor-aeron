package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import reactor.util.Logger;
import reactor.util.Loggers;

public class DataFragmentHandler implements FragmentHandler {

  private final Logger logger = Loggers.getLogger(DataFragmentHandler.class);

  private final DataMessageSubscriber subscriber;

  public DataFragmentHandler(DataMessageSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int index = offset;
    int type = buffer.getByte(index);
    index += BitUtil.SIZE_OF_BYTE;
    long sessionId = buffer.getLong(index);
    index += BitUtil.SIZE_OF_LONG;

    if (type == MessageType.NEXT.ordinal()) {
      int bytesLength = length - (index - offset);
      ByteBuffer dst = ByteBuffer.allocate(bytesLength);
      buffer.getBytes(index, dst, bytesLength);
      dst.rewind();

      subscriber.onNext(sessionId, dst);
    } else {
      logger.error("Unknown message type id: {}", type);
      subscriber.onComplete(sessionId);
    }
  }
}
