package reactor.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface DirectBufferHandler<B> {

  int estimateLength(B buffer);

  void write(MutableDirectBuffer dstBuffer, int index, B srcBuffer, int length);

  DirectBuffer map(B buffer, int length);

  void dispose(B buffer);
}
