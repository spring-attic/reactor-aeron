package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ExclusivePublication;
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
import reactor.core.Exceptions;
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
              logger.error("{} failed to start, cause: {}", this, th);
              dispose();
            });

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
   * @param connectTimeout connect timeout
   * @param backpressureTimeout backpressure timeout
   * @return mono result
   */
  public Mono<MessagePublication> publication(
      String channel, Duration connectTimeout, Duration backpressureTimeout) {

    return Mono.defer(
        () -> {
          Publication pub;
          try {
            pub = aeron.addExclusivePublication(channel, STREAM_ID);
          } catch (Exception ex) {
            logger.error(
                "{} failed on aeron.addExclusivePublication(), channel: {}, cause: {}",
                this,
                channel,
                ex.toString());
            throw Exceptions.propagate(ex);
          }

          AeronEventLoop eventLoop = this.nextEventLoop();

          MessagePublication publication =
              new MessagePublication(pub, eventLoop, connectTimeout, backpressureTimeout);

          return eventLoop
              .registerMessagePublication(publication)
              .doOnError(
                  ex -> {
                    logger.error(
                        "{} failed to register: {}, cause: {}", this, publication, ex.toString());
                    if (!pub.isClosed()) {
                      pub.close();
                    }
                  })
              .thenReturn(publication)
              .doOnSuccess(p -> logger.debug("{} registered: {}", this, p));
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
   * @param fragmentHandler fragment handler
   * @param availableImageHandler available image handler; optional
   * @param unavailableImageHandler unavailable image handler; optional
   * @return mono result
   */
  public Mono<MessageSubscription> subscription(
      String channel,
      FragmentHandler fragmentHandler,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return Mono.defer(
        () -> {
          Subscription sub;
          try {
            sub =
                aeron.addSubscription(
                    channel,
                    STREAM_ID,
                    image -> {
                      logger.debug(
                          "{} onImageAvailable: {} {}",
                          this,
                          Integer.toHexString(image.sessionId()),
                          image.sourceIdentity());
                      if (availableImageHandler != null) {
                        availableImageHandler.accept(image);
                      }
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
          } catch (Exception ex) {
            logger.error(
                "{} failed on aeron.addSubscription(), channel: {}, cause: {}",
                this,
                channel,
                ex.toString());
            throw Exceptions.propagate(ex);
          }

          AeronEventLoop eventLoop = this.nextEventLoop();

          MessageSubscription subscription =
              new MessageSubscription(eventLoop, sub, new FragmentAssembler(fragmentHandler));

          return eventLoop
              .registerMessageSubscription(subscription)
              .doOnError(
                  ex -> {
                    logger.error(
                        "{} failed to register: {}, cause: {}", this, subscription, ex.toString());
                    if (!sub.isClosed()) {
                      sub.close();
                    }
                  })
              .thenReturn(subscription)
              .doOnSuccess(s -> logger.debug("{} registered: {}", this, s));
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
      logger.debug("{} deleted aeron directory {}", this, file);
    }
  }

  @Override
  public String toString() {
    return "AeronResources0x" + Integer.toHexString(System.identityHashCode(this));
  }
}
