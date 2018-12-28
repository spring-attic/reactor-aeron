package reactor.aeron;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public final class DefaultAeronOutbound implements AeronOutbound, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(DefaultAeronOutbound.class);

  private final String category;
  private final String channel;
  private final AeronResources resources;
  private final AeronOptions options;

  private volatile AeronWriteSequencer sequencer;
  private volatile MessagePublication publication;

  /**
   * Constructor.
   *
   * @param category category
   * @param channel channel
   * @param resources resources
   * @param options options
   */
  public DefaultAeronOutbound(
      String category, String channel, AeronResources resources, AeronOptions options) {
    this.category = category;
    this.channel = channel;
    this.resources = resources;
    this.options = options;
  }

  /**
   * Init method. Creates data publication and assigns it to event loop. Makes this object eligible
   * for calling {@link #send(Publisher)} function.
   *
   * @param streamId stream id
   * @return initialization handle
   */
  public Mono<Void> start(int streamId) {
    return Mono.defer(
        () -> {
          final AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .messagePublication(category, channel, streamId, options, eventLoop)
              .doOnSuccess(result -> sequencer = new AeronWriteSequencer(publication = result))
              .flatMap(
                  result -> {
                    Duration retryInterval = Duration.ofMillis(100);
                    Duration connectTimeout = options.connectTimeout();
                    long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

                    return checkConnected()
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .onErrorResume(
                            th -> {
                              logger.warn(
                                  "Failed to connect publication: {} for sending data within {} ms",
                                  publication,
                                  connectTimeout.toMillis());
                              dispose();
                              return Mono.error(th);
                            });
                  });
        });
  }

  private Mono<Void> checkConnected() {
    return Mono.defer(
        () ->
            publication.isConnected()
                ? Mono.empty()
                : Mono.error(
                    AeronExceptions.failWithPublication("aeron.Publication is not connected")));
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    Objects.requireNonNull(sequencer, "sequencer must be initialized");
    return then(sequencer.write(dataStream));
  }

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    if (publication != null) {
      publication.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return publication != null && publication.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return publication != null ? publication.onDispose() : Mono.empty();
  }
}
