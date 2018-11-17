package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import reactor.core.Disposable;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.Pooler;
import reactor.util.Logger;
import reactor.util.Loggers;

public class AeronResources implements Disposable, AutoCloseable {

  private static final Logger logger = Loggers.getLogger(AeronResources.class);

  private final String name;

  private final DriverManager driverManager;
  private final boolean isDriverLaunched;
  private final Aeron aeron;
  private final Pooler pooler;

  private volatile boolean isRunning = true;

  public AeronResources(String name) {
    this(name, null);
  }

  /**
   * Creates aeron resources.
   *
   * @param name name
   * @param aeron aeron
   */
  public AeronResources(String name, Aeron aeron) {
    this.name = name;
    this.driverManager = new DriverManager();
    if (aeron == null) {
      driverManager.launchDriver();
      this.aeron = driverManager.getAeron();
      isDriverLaunched = true;
    } else {
      this.aeron = aeron;
      isDriverLaunched = false;
    }
    this.pooler = new Pooler(name);
    pooler.initialise();
  }

  /**
   * Adds publication. Also see {@link #release(Publication)}}.
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
          "[{}] Added publication{} {} {}",
          category,
          formatSessionId(sessionId),
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return publication;
  }

  /**
   * Adds control subscription and register it in the {@link Pooler}. Also see {@link
   * #release(Subscription)}}.
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
    pooler.addControlSubscription(subscription, controlMessageSubscriber);
    return subscription;
  }

  /**
   * Adds data subscription and register it in the {@link Pooler}. Also see {@link
   * #release(Subscription)}}.
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
    pooler.addDataSubscription(subscription, dataMessageSubscriber);
    return subscription;
  }

  /**
   * Releases the given subscription.
   *
   * @param subscription subscription
   */
  public void release(Subscription subscription) {
    if (subscription != null) {
      pooler.removeSubscription(subscription);
      subscription.close();
    }
  }

  /**
   * Releases the given publication.
   *
   * @param publication publication
   */
  public void release(Publication publication) {
    if (publication != null) {
      publication.close();
    }
  }

  /**
   * Creates new AeronWriteSequencer.
   *
   * @param category category
   * @param publication publication (see {@link #publication(String, String, int, String, long)}
   * @param sessionId session id
   * @return new write sequencer
   */
  public AeronWriteSequencer newWriteSequencer(
      String category, MessagePublication publication, long sessionId) {
    MediaDriver.Context mc = driverManager.getMediaContext();
    if (logger.isDebugEnabled()) {
      logger.debug("[{}] Created aeronWriteSequencer{}", category, formatSessionId(sessionId));
    }
    return new AeronWriteSequencer(mc.senderCommandQueue(), category, publication, sessionId);
  }

  @Override
  public void close() {
    dispose();
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      isRunning = false;
      pooler
          .shutdown()
          .subscribe(
              null,
              th -> {
                /* todo */
              });
      if (isDriverLaunched) {
        driverManager.shutdownDriver().block();
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return !isRunning;
  }

  private Subscription addSubscription(
      String category, String channel, int streamId, String purpose, long sessionId) {
    Subscription subscription = aeron.addSubscription(channel, streamId);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Added subscription{} {} {}",
          category,
          formatSessionId(sessionId),
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return subscription;
  }

  private String formatSessionId(long sessionId) {
    return sessionId > 0 ? " sessionId: " + sessionId : "";
  }
}
