package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import reactor.core.Disposable;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.MessagePublication;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Aeron wrapper. */
public final class AeronWrapper implements Disposable {

  private static final Logger logger = Loggers.getLogger(AeronWrapper.class);

  private final String category;

  // dont try to remove static qualifier! this is damn singleton
  private static final DriverManager driverManager = new DriverManager();

  private final Aeron aeron;

  private final boolean isDriverLaunched;

  /**
   * Constructor.
   *
   * @param category category
   * @param aeron aeron
   */
  public AeronWrapper(String category, Aeron aeron) {
    this.category = category;
    if (aeron == null) {
      driverManager.launchDriver();
      this.aeron = driverManager.getAeron();
      isDriverLaunched = true;
    } else {
      this.aeron = aeron;
      isDriverLaunched = false;
    }
  }

  @Override
  public void dispose() {
    if (isDriverLaunched) {
      driverManager.shutdownDriver().block();
    }
  }

  /**
   * Adds publication.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @return publication
   */
  public Publication addPublication(String channel, int streamId, String purpose, long sessionId) {
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
   * Adds subscription.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @return subscription
   */
  public Subscription addSubscription(
      String channel, int streamId, String purpose, long sessionId) {
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

  /**
   * Creates new AeronWriteSequencer.
   *
   * @param category catergory
   * @param publication publication (see {@link #addPublication(String, int, String, long)}
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

  private String formatSessionId(long sessionId) {
    return sessionId > 0 ? " sessionId: " + sessionId : "";
  }
}
