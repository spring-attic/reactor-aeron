package reactor.aeron;

public class HeartbeatSendFailedException extends RuntimeException {

  private final long sessionId;

  public HeartbeatSendFailedException(long sessionId) {
    this.sessionId = sessionId;
  }
}
