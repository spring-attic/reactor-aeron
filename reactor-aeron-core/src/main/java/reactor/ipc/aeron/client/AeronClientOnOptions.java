package reactor.ipc.aeron.client;

public class AeronClientOnOptions extends AeronClientOperator {

  private final AeronClientOptions options;

  public AeronClientOnOptions(AeronClient source, AeronClientOptions options) {
    super(source);
    this.options = options;
  }

  @Override
  public AeronClientOptions options() {
    return options;
  }
}
