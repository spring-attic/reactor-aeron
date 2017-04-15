package reactor.ipc.aeron;

import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;

import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronHelper {

    private final Logger logger;

    private final static DriverManager driverManager = new DriverManager();

    private final Aeron aeron;

    private final boolean isDriverLaunched;

    public AeronHelper(String category, AeronOptions options) {
        this.logger = Loggers.getLogger(AeronHelper.class + "." + category);

        if (options.getAeron() == null) {
            driverManager.launchDriver();
            aeron = driverManager.getAeron();
            isDriverLaunched = true;
        } else {
            aeron = options.getAeron();
            isDriverLaunched = false;
        }
    }

    public void shutdown() {
        if (isDriverLaunched) {
            driverManager.shutdownDriver().block();
        }
    }

    public Publication addPublication(String channel, int streamId, String purpose, UUID sessionId) {
        Publication publication = aeron.addPublication(channel, streamId);
        logger.debug("Added publication, sessionId: {} for {}: {}/{}", sessionId, purpose, channel, streamId);
        return publication;
    }

    public Subscription addSubscription(String channel, int streamId, String purpose, UUID sessionId) {
        Subscription subscription = aeron.addSubscription(channel, streamId);

        logger.debug("Added subscription{} for {}: {}/{}", sessionId != null ? ", sessionId: " + sessionId: "",
                purpose, channel, streamId);
        return subscription;
    }

}
