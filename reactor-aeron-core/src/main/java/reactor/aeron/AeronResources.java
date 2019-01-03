package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
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
            avoid -> logger.info("{} has started", this),
            th -> {
              logger.error("Start of {} failed with error: {}", this, th);
              dispose();
            });

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            avoid -> logger.info("{} closed", this),
            th -> logger.warn("{} closed with error: {}", this, th.toString()));
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
   * @param channel
   * @param connectTimeout
   * @param backpressureTimeout
   * @return
   */
  public Mono<MessagePublication> publication(
      String channel, Duration connectTimeout, Duration backpressureTimeout) {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = this.nextEventLoop();

          Publication pub = aeron.addExclusivePublication(channel, STREAM_ID);

          MessagePublication publication =
              new MessagePublication(pub, eventLoop, connectTimeout, backpressureTimeout);

          return eventLoop
              .register(publication)
              .doOnError(
                  ex -> {
                    logger.error(
                        "Failed to register publication: {}, cause: {}",
                        publication,
                        ex.toString());
                    if (!pub.isClosed()) {
                      pub.close();
                    }
                  })
              .thenReturn(publication);
        });
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      dispose.onComplete();
    }
  }

  /**
   * @param channel
   * @param fragmentHandler
   * @param availableImageHandler
   * @param unavailableImageHandler
   * @return
   */
  public Mono<MessageSubscription> subscription(
      String channel,
      FragmentHandler fragmentHandler,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = this.nextEventLoop();

          Subscription sub =
              aeron.addSubscription(
                  channel,
                  STREAM_ID,
                  image -> {
                    logger.debug(
                        "onImageAvailable: 0x{} {}",
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    if (availableImageHandler != null) {
                      availableImageHandler.accept(image);
                    }
                    Optional //
                        .ofNullable(availableImageHandler)
                        .ifPresent(c -> c.accept(image));
                  },
                  image -> {
                    logger.debug(
                        "onImageUnavailable: 0x{} {}",
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    Optional //
                        .ofNullable(unavailableImageHandler)
                        .ifPresent(c -> c.accept(image));
                  });

          MessageSubscription subscription =
              new MessageSubscription(eventLoop, sub, new FragmentAssembler(fragmentHandler));

          return eventLoop
              .register(subscription)
              .doOnError(
                  ex -> {
                    logger.error(
                        "Failed to register subscription: {}, cause: {}",
                        subscription,
                        ex.toString());
                    if (!sub.isClosed()) {
                      sub.close();
                    }
                  })
              .thenReturn(subscription);
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
          logger.info("{} shutdown initiated", this);

          eventLoopGroup.dispose();

          return eventLoopGroup
              .onDispose()
              .doFinally(
                  s -> {
                    CloseHelper.quietClose(aeron);
                    CloseHelper.quietClose(mediaDriver);
                    Optional.ofNullable(mediaDriver) //
                        .map(MediaDriver::context)
                        .ifPresent(context -> IoUtil.delete(context.aeronDirectory(), true));

                    logger.info("{} shutdown complete", this);
                  });
        });
  }

  private void deleteAeronDirectory(Context context) {
    File file = context.aeronDirectory();
    if (file.exists()) {
      IoUtil.delete(file, true);
      logger.debug("Deleted aeron directory {}", file);
    }
  }

  @Override
  public String toString() {
    return "AeronResources@" + Integer.toHexString(System.identityHashCode(this));
  }
}
