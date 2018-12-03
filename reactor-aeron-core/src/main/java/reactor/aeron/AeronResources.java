package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.util.Optional;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

public class AeronResources implements Disposable, AutoCloseable {

  private static final Logger logger = Loggers.getLogger(AeronResources.class);

  private final AeronResourcesConfig config;
  private final MonoProcessor<Void> onStart = MonoProcessor.create();
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  private Aeron aeron;
  private MediaDriver mediaDriver;

  private Poller poller;
  private AeronEventLoop eventLoop;
  private Scheduler receiver;

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
        .doOnTerminate(this::onClose)
        .subscribe(
            avoid -> logger.info("{} has stopped", this),
            th -> logger.warn("{} disposed with error: {}", this, th));
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
    receiver = Schedulers.newSingle("reactor-aeron-receiver");

    receiver.schedule(poller = new Poller(() -> !receiver.isDisposed()));

    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> deleteAeronDirectory(aeronContext, directoryName)));

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
   * @return mono handle of creation and registering of message publicartiotn
   */
  public Mono<MessagePublication> messagePublication(
      String category,
      String channel,
      int streamId,
      AeronOptions options,
      AeronEventLoop eventLoop) {

    Publication publication = aeron.addPublication(channel, streamId);

    MessagePublication messagePublication =
        new MessagePublication(category, publication, options, eventLoop);

    return eventLoop
        .register(messagePublication)
        .doOnError(
            ex -> {
              logger.error(
                  "Failed to register publication {} on eventLoop {}, cause: {}",
                  AeronUtils.format(publication),
                  eventLoop,
                  ex);
              publication.close();
            })
        .doOnSuccess(
            avoid -> logger.debug("[{}] Added publication: {}", category, messagePublication))
        .thenReturn(messagePublication);
  }

  /**
   * Adds control subscription and register it in the {@link Poller}. Also see {@link
   * #close(Subscription)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @param controlMessageSubscriber control message subscriber
   * @param onAvailableImage called when {@link Image}s become available for consumption. Null is
   *     valid if no action is to be taken.
   * @param onUnavailableImage called when {@link Image}s go unavailable for consumption. Null is
   *     valid if no action is to be taken.
   * @return control subscription
   */
  public Subscription controlSubscription(
      String category,
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      ControlMessageSubscriber controlMessageSubscriber,
      Consumer<Image> onAvailableImage,
      Consumer<Image> onUnavailableImage) {
    Subscription subscription =
        addSubscription(
            category + "-control",
            channel,
            streamId,
            purpose,
            sessionId,
            onAvailableImage,
            onUnavailableImage);
    poller.addControlSubscription(subscription, controlMessageSubscriber);
    return subscription;
  }

  /**
   * Adds data subscription and register it in the {@link Poller}. Also see {@link
   * #close(Subscription)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @param dataMessageSubscriber data message subscriber
   * @param onAvailableImage called when {@link Image}s become available for consumption. Null is
   *     valid if no action is to be taken.
   * @param onUnavailableImage called when {@link Image}s go unavailable for consumption. Null is
   *     valid if no action is to be taken.
   * @return control subscription
   */
  public Subscription dataSubscription(
      String category,
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      DataMessageSubscriber dataMessageSubscriber,
      Consumer<Image> onAvailableImage,
      Consumer<Image> onUnavailableImage) {
    Subscription subscription =
        addSubscription(
            category + "-data",
            channel,
            streamId,
            purpose,
            sessionId,
            onAvailableImage,
            onUnavailableImage);
    poller.addDataSubscription(subscription, dataMessageSubscriber);
    return subscription;
  }

  /**
   * Closes the given subscription.
   *
   * @param subscription subscription
   */
  public void close(Subscription subscription) {
    // todo wait for commandQueue
    Schedulers.single()
        .schedule(
            () -> {
              if (subscription != null) {
                poller.removeSubscription(subscription);
                try {
                  subscription.close();
                } catch (Exception e) {
                  logger.warn("Subscription closed with error: {}", e);
                }
              }
            });
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

  private void onClose() {
    logger.info("{} shutdown initiated", this);

    //    Optional.ofNullable(sender) //
    //        .filter(s -> !s.isDisposed())
    //        .ifPresent(Scheduler::dispose);
    //
    //    Optional.ofNullable(receiver) //
    //        .filter(s -> !s.isDisposed())
    //        .ifPresent(Scheduler::dispose);
    // TODO at this point close EventLoop

    CloseHelper.quietClose(aeron);

    CloseHelper.quietClose(mediaDriver);

    Optional.ofNullable(mediaDriver) //
        .map(MediaDriver::context)
        .ifPresent(CommonContext::deleteAeronDirectory);

    logger.info("{} shutdown complete", this);
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  /**
   * Adds a subscription by given channel and stream id.
   *
   * @param category category
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @param availableImageConsumer called when {@link Image}s become available for consumption. Null
   *     is valid if no action is to be taken.
   * @param unavailableImageConsumer called when {@link Image}s go unavailable for consumption. Null
   *     is valid if no action is to be taken.
   * @return subscription for channel and stream id
   */
  Subscription addSubscription(
      String category,
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      Consumer<Image> availableImageConsumer,
      Consumer<Image> unavailableImageConsumer) {

    Subscription subscription =
        aeron.addSubscription(
            channel,
            streamId,
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] {} available image, sessionId={}, source={}",
                    category,
                    AeronUtils.format(channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (availableImageConsumer != null) {
                availableImageConsumer.accept(image);
              }
            },
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] {} unavailable image, sessionId={}, source={}",
                    category,
                    AeronUtils.format(channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (unavailableImageConsumer != null) {
                unavailableImageConsumer.accept(image);
              }
            });
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Added subscription, sessionId={} {} {}",
          category,
          sessionId,
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return subscription;
  }

  private void deleteAeronDirectory(Context aeronContext, String directoryName) {
    try {
      aeronContext.deleteAeronDirectory();
    } catch (Throwable th) {
      logger.warn(
          "Exception occurred at deleting aeron directory: {}, cause: {}", directoryName, th);
    }
  }

  @Override
  public String toString() {
    return "AeronResources@" + Integer.toHexString(System.identityHashCode(this));
  }
}
