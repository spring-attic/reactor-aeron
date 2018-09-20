package reactor.ipc.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;

public interface MessagePublication {

  /**
   * Publishes a message into Aeron.
   *
   * @throws IllegalArgumentException as specified for {@link Publication#tryClaim(int,
   *     BufferClaim)}
   * @throws RuntimeException when unexpected exception occurs
   */
  long publish(MessageType msgType, ByteBuffer msgBody, long sessionId);

  String asString();
}
