package reactor.aeron;

import org.reactivestreams.Publisher;

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
  public AeronOutbound send(Publisher<?> dataStream) {
    return then(sequencer.write(dataStream));
  }

  void dispose() {
    publication.dispose();
  }
}
