package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public final class DefaultAeronOutbound implements AeronOutbound {

  private final AeronWriteSequencer sequencer;

  /**
   * Constructor.
   *
   * @param publication message publication
   */
  public DefaultAeronOutbound(MessagePublication publication) {
    this.sequencer = new AeronWriteSequencer(publication);
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(sequencer.write(dataStream));
  }

  @Override
  public AeronOutbound sendString(Publisher<String> dataStream) {
    return send(Flux.from(dataStream).map(String::getBytes).map(ByteBuffer::wrap));
  }
}
