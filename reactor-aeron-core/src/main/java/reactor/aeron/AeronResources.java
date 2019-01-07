package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
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

public class AeronResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  /** The stream ID that the server and client use for messages. */
  private static final int STREAM_ID = 0xcafe0000;

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
        .doOnTerminate(this::doStart)
        .subscribe(
            null,
            th -> {
              logger.error("{} failed to start, cause: {}", this, th.toString());
              dispose();
            },
            () -> logger.info("{} started", this));

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} closed with error: {}", this, th.toString()),
            () -> logger.info("{} closed", this));
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
  public static AeronResources start(AeronResourcesConfig config) {
    AeronResources aeronResources = new AeronResources(config);
    aeronResources.start0();
    return aeronResources;
  }

  private void start0() {
    if (!isDisposed()) {
      start.onComplete();
    }
  }

  private void doStart() {
    MediaDriver.Context mediaContext =
        new MediaDriver.Context()
            .aeronDirectoryName(config.aeronDirectoryName())
            .mtuLength(config.mtuLength())
            .imageLivenessTimeoutNs(config.imageLivenessTimeout().toNanos())
            .dirDeleteOnStart(config.isDirDeleteOnStart());

    mediaDriver = MediaDriver.launchEmbedded(mediaContext);

    Aeron.Context aeronContext = new Aeron.Context();
    String directoryName = mediaDriver.aeronDirectoryName();
    aeronContext.aeronDirectoryName(directoryName);

    aeron = Aeron.connect(aeronContext);

    eventLoopGroup = new AeronEventLoopGroup(config.idleStrategySupplier(), config.numOfWorkers());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteAeronDirectory(aeronContext)));

    logger.info(
        "{} has initialized embedded media mediaDriver, aeron directory: {}", this, directoryName);
  }

  public AeronEventLoop nextEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Creates aeron {@link ExclusivePublication} then wraps it into {@link MessagePublication}.
   * Result message publication will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param options aeorn options
   * @return mono result
   */
  public Mono<MessagePublication> publication(String channel, AeronOptions options) {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = this.nextEventLoop();

          return eventLoop
              .publicationFromSupplier(() -> aeron.addExclusivePublication(channel, STREAM_ID))
              .doOnError(
                  ex ->
                      logger.error(
                          "{} failed on addExclusivePublication(), channel: {}, cause: {}",
                          this,
                          channel,
                          ex.toString()))
              .flatMap(
                  aeronPublication ->
                      eventLoop
                          .registerPublication(
                              new MessagePublication(aeronPublication, options, eventLoop))
                          .doOnError(
                              ex -> {
                                logger.error(
                                    "{} failed to register message publication, cause: {}",
                                    this,
                                    ex.toString());
                                if (!aeronPublication.isClosed()) {
                                  aeronPublication.close();
                                }
                              }));
        });
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      dispose.onComplete();
    }
  }

  /**
   * Creates aeron {@link Subscription} then wraps it into {@link MessageSubscription}. Result
   * message subscription will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param options aeron options
   * @param fragmentHandler fragment handler
   * @param availableImageHandler available image handler; optional
   * @param unavailableImageHandler unavailable image handler; optional
   * @return mono result
   */
  public Mono<MessageSubscription> subscription(
      String channel,
      AeronOptions options,
      FragmentHandler fragmentHandler,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = this.nextEventLoop();

          return eventLoop
              .subscriptionFromSupplier(
                  () -> aeronSubscription(channel, availableImageHandler, unavailableImageHandler))
              .timeout(Duration.ofSeconds(3))
              .doOnError(
                  ex ->
                      logger.error(
                          "{} failed on aeronSubscription(), channel: {}, cause: {}",
                          this,
                          channel,
                          ex.toString()))
              .flatMap(
                  aeronSubscription ->
                      eventLoop
                          .registerSubscription(
                              new MessageSubscription(
                                  aeronSubscription,
                                  options,
                                  eventLoop,
                                  new FragmentAssembler(fragmentHandler)))
                          .doOnError(
                              ex -> {
                                logger.error(
                                    "{} failed to register message subscription, cause: {}",
                                    this,
                                    ex.toString());
                                if (!aeronSubscription.isClosed()) {
                                  aeronSubscription.close();
                                }
                              }))
              .timeout(Duration.ofSeconds(4));
        });
  }

  private Subscription aeronSubscription(
      String channel,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {
    logger.debug("Adding subscription for channel {}", channel);
    long startTime = System.nanoTime();
    Subscription subscription =
        aeron.addSubscription(
            channel,
            STREAM_ID,
            image -> {
              logger.debug(
                  "{} onImageAvailable: {} {}",
                  this,
                  Integer.toHexString(image.sessionId()),
                  image.sourceIdentity());
              Optional.ofNullable(availableImageHandler).ifPresent(c -> c.accept(image));
            },
            image -> {
              logger.debug(
                  "{} onImageUnavailable: {} {}",
                  this,
                  Integer.toHexString(image.sessionId()),
                  image.sourceIdentity());
              Optional.ofNullable(unavailableImageHandler).ifPresent(c -> c.accept(image));
            });
    long endTime = System.nanoTime();
    Duration spent = Duration.ofNanos(endTime - startTime);
    logger.debug("Added subscription for channel {}, spent: {} ns", channel, spent);
    return subscription;
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
          logger.info("{} shutting down", this);

          eventLoopGroup.dispose();

          return eventLoopGroup
              .onDispose()
              .doFinally(
                  s -> {
                    CloseHelper.quietClose(aeron);

                    CloseHelper.quietClose(mediaDriver);

                    Optional.ofNullable(mediaDriver)
                        .map(MediaDriver::context)
                        .ifPresent(context -> IoUtil.delete(context.aeronDirectory(), true));

                    logger.info("{} has shutdown", this);
                  });
        });
  }

  private void deleteAeronDirectory(Context context) {
    File file = context.aeronDirectory();
    if (file.exists()) {
      IoUtil.delete(file, true);
      logger.debug("{} deleted aeron directory {}", this, file);
    }
  }

  @Override
  public String toString() {
    return "AeronResources0x" + Integer.toHexString(System.identityHashCode(this));
  }
}
