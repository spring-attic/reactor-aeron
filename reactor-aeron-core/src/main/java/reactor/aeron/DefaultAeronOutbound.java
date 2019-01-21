package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class DefaultAeronOutbound implements AeronOutbound {

  private final AeronWriteSequencer sequencer;
  private final MessagePublication publication;

  /**
   * Constructor.
   *
   * @param publication message publication
   */
  DefaultAeronOutbound(MessagePublication publication) {
    this.publication = publication;
    this.sequencer = new AeronWriteSequencer(publication);
  }

  @Override
  public <B> AeronOutbound send(
      Publisher<B> dataStream, DirectBufferHandler<? super B> bufferHandler) {
    return then(sequencer.write(dataStream, bufferHandler));
  }

  @Override
  public AeronOutbound send(Publisher<DirectBuffer> dataStream) {
    return then(sequencer.write(dataStream));
  }

  @Override
  public AeronOutbound sendBytes(Publisher<byte[]> dataStream) {
    if (dataStream instanceof Flux) {
      return send(((Flux<byte[]>) dataStream).map(UnsafeBuffer::new));
    }
    return send(((Mono<byte[]>) dataStream).map(UnsafeBuffer::new));
  }

  @Override
  public AeronOutbound sendString(Publisher<String> dataStream) {
    if (dataStream instanceof Flux) {
      return send(
          ((Flux<String>) dataStream)
              .map(s -> s.getBytes(StandardCharsets.UTF_8))
              .map(UnsafeBuffer::new));
    }
    return send(
        ((Mono<String>) dataStream)
            .map(s -> s.getBytes(StandardCharsets.UTF_8))
            .map(UnsafeBuffer::new));
  }

  @Override
  public AeronOutbound sendBuffer(Publisher<ByteBuffer> dataStream) {
    if (dataStream instanceof Flux) {
      return send(((Flux<ByteBuffer>) dataStream).map(UnsafeBuffer::new));
    }
    return send(((Mono<ByteBuffer>) dataStream).map(UnsafeBuffer::new));
  }

  void dispose() {
    publication.dispose();
  }
}
