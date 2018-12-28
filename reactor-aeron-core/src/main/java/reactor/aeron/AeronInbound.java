package reactor.aeron;

import java.nio.ByteBuffer;
import reactor.core.publisher.Flux;

public interface AeronInbound {

  Flux<ByteBuffer> receive();

  default Flux<String> receiveAsString() {
    return receive().map(AeronUtils::byteBufferToString);
  }
}
