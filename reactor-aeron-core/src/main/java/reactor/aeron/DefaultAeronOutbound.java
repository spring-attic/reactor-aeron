package reactor.aeron;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;

/** Default aeron outbound. */
public final class DefaultAeronOutbound implements Disposable, AeronOutbound {

  private final String category;

  private final AeronResources aeronResources;

  private final String channel;

  private final AeronOptions options;

  private volatile AeronWriteSequencer sequencer;

  private volatile DefaultMessagePublication publication;

  /**
   * Constructor.
   *
   * @param category category
   * @param aeronResources aeronResources
   * @param channel channel
   * @param options options
   */
  public DefaultAeronOutbound(
      String category, AeronResources aeronResources, String channel, AeronOptions options) {
    this.category = category;
    this.aeronResources = aeronResources;
    this.channel = channel;
    this.options = options;
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(sequencer.write(dataStream));
  }

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    if (publication != null && !publication.isDisposed()) {
      publication.dispose();
    }
  }

  /**
   * Init method.
   *
   * @param sessionId session id
   * @param streamId stream id
   * @return initialization handle
   */
  public Mono<Void> initialise(long sessionId, int streamId) {
    return Mono.create(
        sink -> {
          Publication aeronPublication =
              aeronResources.publication(category, channel, streamId, "to send data to", sessionId);
          this.publication =
              new DefaultMessagePublication(
                  aeronResources.eventLoop(), aeronPublication, category, options);
          this.sequencer = aeronResources.writeSequencer(category, publication, sessionId);
          int timeoutMillis = options.connectTimeoutMillis();

          createRetryTask(sink, aeronPublication, timeoutMillis).schedule();
        });
  }

  private RetryTask createRetryTask(
      MonoSink<Void> sink, Publication aeronPublication, int timeoutMillis) {
    return new RetryTask(
        Schedulers.single(),
        100,
        timeoutMillis,
        () -> {
          if (aeronPublication.isConnected()) {
            sink.success();
            return true;
          }
          return false;
        },
        throwable -> {
          String errMessage =
              String.format(
                  "Publication %s for sending data in not connected during %d millis",
                  publication, timeoutMillis);
          sink.error(new Exception(errMessage, throwable));
        });
  }

  public MessagePublication getPublication() {
    return publication;
  }
}
