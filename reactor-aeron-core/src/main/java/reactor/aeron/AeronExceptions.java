package reactor.aeron;

public class AeronExceptions {

  private AeronExceptions() {
    // Do not instantiate
  }

  public static RuntimeException failWithCancel(String message) {
    return new AeronCancelException(message);
  }

  public static RuntimeException failWithEventLoopUnavailable() {
    return new AeronEventLoopException("AeronEventLoop unavailable");
  }

  public static RuntimeException failWithMessagePublicationUnavailable() {
    return new MessagePublicationException("MessagePublication unavailable");
  }

  public static RuntimeException failWithPublication(String message) {
    return new AeronPublicationException(message);
  }

  static class AeronCancelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    AeronCancelException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class AeronEventLoopException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public AeronEventLoopException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class MessagePublicationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MessagePublicationException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class AeronPublicationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public AeronPublicationException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
