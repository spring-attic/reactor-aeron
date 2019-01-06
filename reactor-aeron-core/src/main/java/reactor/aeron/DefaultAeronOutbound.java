package reactor.aeron;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class DefaultAeronOutbound implements AeronOutbound, OnDisposable {

  private final MessagePublication publication;
  private final AeronWriteSequencer sequencer;

  /**
   * Constructor. Stores passed {@link MessagePublication} and creates {@link AeronWriteSequencer}
   * instance upon it. Stored message publication is bound to lifecycle of {@link
   * DefaultAeronOutbound} instance. Corresponding dispose functions shutting down message
   * publication.
   *
   * @param publication message publication
   */
  public DefaultAeronOutbound(MessagePublication publication) {
    this.publication = publication;
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

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    publication.dispose();
  }

  @Override
  public boolean isDisposed() {
    return publication.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return publication.onDispose();
  }
}
