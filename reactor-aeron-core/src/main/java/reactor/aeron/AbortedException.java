package reactor.aeron;

public final class AbortedException extends RuntimeException {

  public AbortedException(String message) {
    super(message);
  }
}
