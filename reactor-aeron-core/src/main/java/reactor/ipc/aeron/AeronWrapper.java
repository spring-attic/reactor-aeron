package reactor.ipc.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class AeronWrapper implements Disposable {

  private static final Logger logger = Loggers.getLogger(AeronWrapper.class);

  private final String category;

  private static final DriverManager driverManager = new DriverManager();

  private final Aeron aeron;

  private final boolean isDriverLaunched;

  public AeronWrapper(String category, AeronOptions options) {
    this.category = category;
    if (options.getAeron() == null) {
      driverManager.launchDriver();
      aeron = driverManager.getAeron();
      isDriverLaunched = true;
    } else {
      aeron = options.getAeron();
      isDriverLaunched = false;
    }
  }

  @Override
  public void dispose() {
    if (isDriverLaunched) {
      driverManager.shutdownDriver().block();
    }
  }

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

  private String formatSessionId(long sessionId) {
    return sessionId > 0 ? " sessionId: " + sessionId : "";
  }
}
