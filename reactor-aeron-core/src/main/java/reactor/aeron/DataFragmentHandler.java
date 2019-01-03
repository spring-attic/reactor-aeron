package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;

public class DataFragmentHandler implements FragmentHandler {

  private final DataMessageSubscriber subscriber;

  public DataFragmentHandler(DataMessageSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {


    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
    buffer.getBytes(offset, dstBuffer, length);
    dstBuffer.rewind();

    subscriber.onNext(dstBuffer);
  }
}
