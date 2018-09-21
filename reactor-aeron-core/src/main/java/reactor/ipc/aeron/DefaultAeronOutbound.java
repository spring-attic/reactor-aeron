package reactor.ipc.aeron;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Default aeron outbound. */
public final class DefaultAeronOutbound implements Disposable, AeronOutbound {

  private final Scheduler scheduler;

  private final String category;

  private final AeronWrapper wrapper;

  private final String channel;

  private final AeronOptions options;

  private volatile WriteSequencer<ByteBuffer> sequencer;

  private volatile DefaultMessagePublication publication;

  public DefaultAeronOutbound(
      String category, AeronWrapper wrapper, String channel, AeronOptions options) {
    this.category = category;
    this.wrapper = wrapper;
    this.channel = channel;
    this.options = options;
    this.scheduler = Schedulers.newSingle(category + "-[sender]", false);
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(sequencer.add(dataStream));
  }

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    scheduler.dispose();

    if (publication != null) {
      publication.dispose();
    }
  }

  public Mono<Void> initialise(long sessionId, int streamId) {
    return Mono.create(
        sink -> {
          Publication aeronPublication =
              wrapper.addPublication(channel, streamId, "to send data to", sessionId);
          this.publication =
              new DefaultMessagePublication(
                  aeronPublication,
                  category,
                  options.connectTimeoutMillis(),
                  options.backpressureTimeoutMillis());
          this.sequencer = new AeronWriteSequencer(scheduler, category, publication, sessionId);
          int timeoutMillis = options.connectTimeoutMillis();
          new RetryTask(
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
                  th ->
                      sink.error(
                          new Exception(
                              String.format(
                                  "Publication %s for sending data in not connected during %d millis",
                                  publication.asString(), timeoutMillis),
                              th)))
              .schedule();
        });
  }

  public MessagePublication getPublication() {
    return publication;
  }
}
