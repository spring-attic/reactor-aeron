package reactor.ipc.aeron;

/**
 * @author Anatoly Kadyshev
 */
public class HeartbeatSendFailedException extends RuntimeException {

    private final long sessionId;

    public HeartbeatSendFailedException(long sessionId) {
        this.sessionId = sessionId;
    }

}
