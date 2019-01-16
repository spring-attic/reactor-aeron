package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

public class AeronResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  private final AeronResourcesConfig config;

  private final MonoProcessor<Void> start = MonoProcessor.create();
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private Aeron aeron;
  private MediaDriver mediaDriver;
  private AeronEventLoopGroup eventLoopGroup;

  private AeronResources(AeronResourcesConfig config) {
    this.config = config;

    start
        .then(doStart())
        .subscribe(
            null,
            th -> {
              logger.error("{} failed to start, cause: {}", this, th.toString());
              dispose();
            });

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  /**
   * Starts aeron resources with default config.
   *
   * @return started instance of aeron resources
   */
  public static AeronResources start() {
    return start(AeronResourcesConfig.defaultConfig());
  }

  /**
   * Starts aeron resources with given config.
   *
   * @param config aeron config
   * @return started instance of aeron resources
   */
  // TODO refactor to comply with approach est. at
  // AeronServer.bind(UnaryOperator<reactor.aeron.AeronOptions>)
  public static AeronResources start(AeronResourcesConfig config) {
    AeronResources aeronResources = new AeronResources(config);
    aeronResources.start0();
    return aeronResources;
  }

  private void start0() {
    start.onComplete();
  }

  private Mono<Void> doStart() {
    return Mono.fromRunnable(
        () -> {
          MediaDriver.Context mediaContext =
              new MediaDriver.Context()
                  .aeronDirectoryName(config.aeronDirectoryName())
                  .mtuLength(config.mtuLength())
                  .imageLivenessTimeoutNs(config.imageLivenessTimeout().toNanos())
                  .dirDeleteOnStart(config.isDirDeleteOnStart());

          mediaDriver = MediaDriver.launchEmbedded(mediaContext);

          Aeron.Context aeronContext = new Aeron.Context();
          aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());

          aeron = Aeron.connect(aeronContext);

          eventLoopGroup =
              new AeronEventLoopGroup(
                  "reactor-aeron", config.numOfWorkers(), config.idleStrategySupplier());

          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(() -> deleteAeronDirectory(aeronContext.aeronDirectory())));

          logger.debug(
              "{} has initialized embedded media driver, aeron directory: {}",
              this,
              aeronContext.aeronDirectoryName());
        });
  }

  /**
   * Creates and registers {@link DefaultAeronInbound}.
   *
   * @param image aeron image
   * @param subscription subscription
   * @return mono result
   */
  Mono<DefaultAeronInbound> inbound(Image image, MessageSubscription subscription) {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = eventLoopGroup.next();
          DefaultAeronInbound inbound = new DefaultAeronInbound(image, eventLoop, subscription);
          return eventLoop
              .registerInbound(inbound)
              .doOnError(
                  ex ->
                      logger.error(
                          "{} failed on registerInbound(), cause: {}", this, ex.toString()));
        });
  }

  /**
   * Creates aeron {@link ExclusivePublication} then wraps it into {@link MessagePublication}.
   * Result message publication will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @param options aeorn options
   * @return mono result
   */
  Mono<MessagePublication> publication(String channel, int streamId, AeronOptions options) {
    return Mono.defer(
        () ->
            aeronPublication(channel, streamId)
                .subscribeOn(Schedulers.parallel())
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronPublication(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString()))
                .flatMap(
                    aeronPublication -> {
                      AeronEventLoop eventLoop = eventLoopGroup.next();
                      return eventLoop
                          .registerPublication(
                              new MessagePublication(aeronPublication, options, eventLoop))
                          .doOnError(
                              ex -> {
                                logger.error(
                                    "{} failed on registerPublication(), cause: {}",
                                    this,
                                    ex.toString());
                                if (!aeronPublication.isClosed()) {
                                  aeronPublication.close();
                                }
                              });
                    }));
  }

  private Mono<Publication> aeronPublication(String channel, int streamId) {
    return Mono.fromCallable(
        () -> {
          logger.debug("Adding aeron.Publication for channel {}", channel);
          long startTime = System.nanoTime();

          Publication publication = aeron.addExclusivePublication(channel, streamId);

          long endTime = System.nanoTime();
          long spent = Duration.ofNanos(endTime - startTime).toNanos();
          logger.debug("Added aeron.Publication for channel {}, spent: {} ns", channel, spent);

          return publication;
        });
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  /**
   * Creates aeron {@link Subscription} then wraps it into {@link MessageSubscription}. Result
   * message subscription will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @param onImageAvailable available image handler; optional
   * @param onImageUnavailable unavailable image handler; optional
   * @return mono result
   */
  Mono<MessageSubscription> subscription(
      String channel,
      int streamId,
      Consumer<Image> onImageAvailable,
      Consumer<Image> onImageUnavailable) {

    return Mono.defer(
        () ->
            aeronSubscription(channel, streamId, onImageAvailable, onImageUnavailable)
                .subscribeOn(Schedulers.parallel())
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronSubscription(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString()))
                .flatMap(
                    aeronSubscription -> {
                      AeronEventLoop eventLoop = eventLoopGroup.next();
                      return eventLoop
                          .registerSubscription(
                              new MessageSubscription(aeronSubscription, eventLoop))
                          .doOnError(
                              ex -> {
                                logger.error(
                                    "{} failed on registerSubscription(), cause: {}",
                                    this,
                                    ex.toString());
                                if (!aeronSubscription.isClosed()) {
                                  aeronSubscription.close();
                                }
                              });
                    }));
  }

  private Mono<Subscription> aeronSubscription(
      String channel,
      int streamId,
      Consumer<Image> onImageAvailable,
      Consumer<Image> onImageUnavailable) {

    return Mono.fromCallable(
        () -> {
          logger.debug("Adding aeron.Subscription for channel {}", channel);
          long startTime = System.nanoTime();

          Subscription subscription =
              aeron.addSubscription(
                  channel,
                  streamId,
                  image -> {
                    logger.debug(
                        "{} onImageAvailable: {} {}",
                        this,
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    Optional.ofNullable(onImageAvailable).ifPresent(c -> c.accept(image));
                  },
                  image -> {
                    logger.debug(
                        "{} onImageUnavailable: {} {}",
                        this,
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    Optional.ofNullable(onImageUnavailable).ifPresent(c -> c.accept(image));
                  });

          long endTime = System.nanoTime();
          long spent = Duration.ofNanos(endTime - startTime).toNanos();
          logger.debug("Added aeron.Subscription for channel {}, spent: {} ns", channel, spent);

          return subscription;
        });
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.defer(
        () -> {
          logger.debug("Disposing {}", this);

          return Mono //
              .fromRunnable(eventLoopGroup::dispose)
              .then(eventLoopGroup.onDispose())
              .doFinally(
                  s -> {
                    CloseHelper.quietClose(aeron);

                    CloseHelper.quietClose(mediaDriver);

                    Optional.ofNullable(mediaDriver)
                        .map(MediaDriver::context)
                        .ifPresent(context -> IoUtil.delete(context.aeronDirectory(), true));
                  });
        });
  }

  private void deleteAeronDirectory(File aeronDirectory) {
    if (aeronDirectory.exists()) {
      IoUtil.delete(aeronDirectory, true);
      logger.debug("{} deleted aeron directory {}", this, aeronDirectory);
    }
  }

  @Override
  public String toString() {
    return "AeronResources0x" + Integer.toHexString(System.identityHashCode(this));
  }
}
