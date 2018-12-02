package reactor.aeron;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Default aeron outbound. */
public final class DefaultAeronOutbound implements OnDisposable, AeronOutbound {

  private static final Logger logger = Loggers.getLogger(DefaultAeronOutbound.class);

  private static final RuntimeException NOT_CONNECTED_EXCEPTION =
      new RuntimeException("publication is not connected");

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

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(Objects.requireNonNull(sequencer).write(dataStream));
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

  private void setPublication(MessagePublication publication) {
    this.publication = publication;
  }

  private void setSequencer(AeronWriteSequencer sequencer) {
    this.sequencer = sequencer;
  }

  /**
   * Init method.
   *
   * @param sessionId session id
   * @param streamId stream id
   * @return initialization handle
   */
  public Mono<Void> initialise(long sessionId, int streamId) {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .messagePublication(category, channel, sessionId, streamId, options, eventLoop)
              .doOnSuccess(this::setPublication)
              .doOnSuccess(
                  result ->
                      setSequencer(new AeronWriteSequencer(sessionId, publication, eventLoop)))
              .flatMap(
                  result -> {
                    Duration retryInterval = Duration.ofMillis(100);
                    Duration connectTimeout = options.connectTimeout();
                    long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

                    return Mono.fromCallable(publication::isConnected)
                        .filter(isConnected -> isConnected)
                        .switchIfEmpty(Mono.error(NOT_CONNECTED_EXCEPTION))
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .onErrorResume(
                            throwable -> {
                              logger.warn(
                                  "Failed to connect publication {} for sending data during {}",
                                  publication,
                                  connectTimeout);
                              return Mono.error(throwable);
                            });
                  });
        });
  }
}
