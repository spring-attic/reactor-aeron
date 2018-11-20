package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.util.Optional;
import org.agrona.CloseHelper;
import reactor.core.Disposable;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.Poller;
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
  private Scheduler sender;
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
    MediaDriver.Context mediaContext = new MediaDriver.Context();
    mediaContext.dirDeleteOnStart(config.isDirDeleteOnStart());
    mediaDriver = MediaDriver.launchEmbedded(mediaContext);

    Aeron.Context aeronContext = new Aeron.Context();
    String directoryName = mediaDriver.aeronDirectoryName();
    aeronContext.aeronDirectoryName(directoryName);

    aeron = Aeron.connect(aeronContext);

    sender = Schedulers.newSingle("reactor-aeron-sender");
    receiver = Schedulers.newSingle("reactor-aeron-receiver");

    receiver.schedule(poller = new Poller(() -> !receiver.isDisposed()));

    logger.info(
        "{} has initialized embedded media mediaDriver, aeron directory: {}", this, directoryName);
  }

  /**
   * Adds publication. Also see {@link #close(Publication)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @return publication
   */
  public Publication publication(
      String category, String channel, int streamId, String purpose, long sessionId) {

    Publication publication = aeron.addPublication(channel, streamId);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Added publication, sessionId={} {} {}",
          category,
          sessionId,
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return publication;
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
   * @return control subscription
   */
  public Subscription controlSubscription(
      String category,
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      ControlMessageSubscriber controlMessageSubscriber) {
    Subscription subscription = addSubscription(category, channel, streamId, purpose, sessionId);
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
   * @return control subscription
   */
  public Subscription dataSubscription(
      String category,
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      DataMessageSubscriber dataMessageSubscriber) {
    Subscription subscription = addSubscription(category, channel, streamId, purpose, sessionId);
    poller.addDataSubscription(subscription, dataMessageSubscriber);
    return subscription;
  }

  /**
   * Closes the given subscription.
   *
   * @param subscription subscription
   */
  public void close(Subscription subscription) {
    if (subscription != null) {
      poller.removeSubscription(subscription);
      CloseHelper.quietClose(subscription);
    }
  }

  /**
   * Closes the given publication.
   *
   * @param publication publication
   */
  public void close(Publication publication) {
    CloseHelper.quietClose(publication);
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

    Optional.ofNullable(sender) //
        .filter(s -> !s.isDisposed())
        .ifPresent(Scheduler::dispose);

    Optional.ofNullable(receiver) //
        .filter(s -> !s.isDisposed())
        .ifPresent(Scheduler::dispose);

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
   * Creates new {@link AeronWriteSequencer}.
   *
   * @param category category
   * @param publication publication (see {@link #publication(String, String, int, String, long)}
   * @param sessionId session id
   * @return new write sequencer
   */
  public AeronWriteSequencer writeSequencer(
      String category, MessagePublication publication, long sessionId) {
    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(sender, category, publication, sessionId);
    if (logger.isDebugEnabled()) {
      logger.debug("[{}] Created {}, sessionId={}", category, writeSequencer, sessionId);
    }
    return writeSequencer;
  }

  /**
   * Adds a subscription by given channel and stream id.
   *
   * @param category category
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @return subscription for channel and stream id
   */
  Subscription addSubscription(
      String category, String channel, int streamId, String purpose, long sessionId) {
    Subscription subscription = aeron.addSubscription(channel, streamId);
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

  @Override
  public String toString() {
    return "AeronResources@" + Integer.toHexString(System.identityHashCode(this));
  }
}
