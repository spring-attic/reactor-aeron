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
import java.util.Optional;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

public class AeronResources implements Disposable, AutoCloseable {

  private static final Logger logger = Loggers.getLogger(AeronResources.class);

  private final AeronResourcesConfig config;

  private final MonoProcessor<Void> onStart = MonoProcessor.create();
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  private Aeron aeron;
  private MediaDriver mediaDriver;
  private AeronEventLoop eventLoop;

  private AeronResources(AeronResourcesConfig config) {
    this.config = config;

    onStart
        .doOnTerminate(this::onStart)
        .subscribe(
            avoid -> logger.info("{} has started", this),
            th -> {
              logger.error("Start of {} failed with error: {}", this, th);
              dispose();
            });

    onClose
        .then(onClose())
        .subscribe(
            avoid -> logger.info("{} closed", this),
            th -> logger.warn("{} closed with error: {}", this, th));
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
      onStart.onComplete();
    }
  }

  private void onStart() {
    MediaDriver.Context mediaContext =
        new MediaDriver.Context()
            .mtuLength(config.mtuLength())
            .imageLivenessTimeoutNs(config.imageLivenessTimeout().toNanos())
            .dirDeleteOnStart(config.isDirDeleteOnStart());

    mediaDriver = MediaDriver.launchEmbedded(mediaContext);

    Aeron.Context aeronContext = new Aeron.Context();
    String directoryName = mediaDriver.aeronDirectoryName();
    aeronContext.aeronDirectoryName(directoryName);

    aeron = Aeron.connect(aeronContext);

    eventLoop = new AeronEventLoop();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteAeronDirectory(aeronContext)));

    logger.info(
        "{} has initialized embedded media mediaDriver, aeron directory: {}", this, directoryName);
  }

  public AeronEventLoop nextEventLoop() {
    return eventLoop;
  }

  /**
   * Adds and registers new message publication.
   *
   * @param category category
   * @param channel channel
   * @param streamId stream id
   * @param options options
   * @param eventLoop event loop where publocation would be registered
   * @return mono handle of creation and registering of message publication
   */
  public Mono<MessagePublication> messagePublication(
      String category,
      String channel,
      int streamId,
      AeronOptions options,
      AeronEventLoop eventLoop) {

    Publication publication = aeron.addPublication(channel, streamId);

    MessagePublication messagePublication =
        new MessagePublication(category, config.mtuLength(), publication, options, eventLoop);

    return eventLoop
        .register(messagePublication)
        .doOnError(
            ex -> {
              logger.error(
                  "[{}] Failed to register publication {} on eventLoop {}, cause: {}",
                  category,
                  AeronUtils.format(publication),
                  eventLoop,
                  ex);
              if (!publication.isClosed()) {
                publication.close();
              }
            })
        .doOnSuccess(
            avoid -> logger.debug("[{}] Added publication: {}", category, messagePublication))
        .thenReturn(messagePublication);
  }

  /**
   * Adds control subscription and register it.
   *
   * @param channel channel
   * @param streamId stream id
   * @param subscriber control message subscriber
   * @param eventLoop event loop where to assign control subscription
   * @param availableImageHandler called when {@link Image}s become available for consumption. Null
   *     is valid if no action is to be taken.
   * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null
   *     is valid if no action is to be taken.
   * @return mono handle of creation and registering of control message subscription
   */
  public Mono<InnerPoller> controlSubscription(
      String category,
      String channel,
      int streamId,
      ControlMessageSubscriber subscriber,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return messageSubscription(
        category + "-control",
        channel,
        streamId,
        subscriber,
        new ControlFragmentHandler(subscriber),
        eventLoop,
        availableImageHandler,
        unavailableImageHandler);
  }

  /**
   * Adds data subscription and register it.
   *
   * @param channel channel
   * @param streamId stream id
   * @param subscriber data message subscriber
   * @param eventLoop event loop where to assign data subscription
   * @param availableImageHandler called when {@link Image}s become available for consumption. Null
   *     is valid if no action is to be taken.
   * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null
   *     is valid if no action is to be taken.
   * @return mono handle of creation and registering of data message subscription
   */
  public Mono<InnerPoller> dataSubscription(
      String category,
      String channel,
      int streamId,
      DataMessageSubscriber subscriber,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return messageSubscription(
        category + "-data",
        channel,
        streamId,
        subscriber,
        new DataFragmentHandler(subscriber),
        eventLoop,
        availableImageHandler,
        unavailableImageHandler);
  }

  @Override
  public void close() {
    dispose();
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      onClose.onComplete();
    }
  }

  private Mono<Void> onClose() {
    return Mono.defer(
        () -> {
          logger.info("{} shutdown initiated", this);

          eventLoop.dispose();

          return eventLoop
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

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  private Mono<InnerPoller> messageSubscription(
      String category,
      String channel,
      int streamId,
      PollerSubscriber subscriber,
      FragmentHandler fragmentHandler,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    Subscription subscription =
        aeron.addSubscription(
            channel,
            streamId,
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] {} available image, imageSessionId={}, imageSource={}",
                    category,
                    AeronUtils.format(channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (availableImageHandler != null) {
                availableImageHandler.accept(image);
              }
            },
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] {} unavailable image, imageSessionId={}, imageSource={}",
                    category,
                    AeronUtils.format(channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (unavailableImageHandler != null) {
                unavailableImageHandler.accept(image);
              }
            });

    InnerPoller innerPoller =
        new InnerPoller(eventLoop, subscription, new FragmentAssembler(fragmentHandler));

    subscriber.onSubscribe(innerPoller);

    return eventLoop
        .register(innerPoller)
        .doOnError(
            ex -> {
              logger.error(
                  "[{}] Failed to register subscription {} on eventLoop {}, cause: {}",
                  category,
                  AeronUtils.format(subscription),
                  eventLoop,
                  ex);
              if (!subscription.isClosed()) {
                subscription.close();
              }
            })
        .doOnSuccess(
            avoid ->
                logger.debug(
                    "[{}] Added subscription: {}", category, AeronUtils.format(subscription)))
        .thenReturn(innerPoller);
  }

  private void deleteAeronDirectory(Context context) {
    File file = context.aeronDirectory();
    if (file.exists()) {
      IoUtil.delete(file, true);
    }
  }

  @Override
  public String toString() {
    return "AeronResources@" + Integer.toHexString(System.identityHashCode(this));
  }
}
