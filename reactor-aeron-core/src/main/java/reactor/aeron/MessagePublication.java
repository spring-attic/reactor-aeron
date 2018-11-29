package reactor.aeron;

import java.nio.ByteBuffer;
import reactor.core.publisher.Mono;

public interface MessagePublication extends AutoCloseable {

  Mono<Void> enqueue(MessageType msgType, ByteBuffer msgBody, long sessionId);

  boolean proceed();

  void close();
}
