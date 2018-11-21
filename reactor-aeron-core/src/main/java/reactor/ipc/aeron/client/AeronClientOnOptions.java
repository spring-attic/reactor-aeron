package reactor.ipc.aeron.client;

class AeronClientOnOptions extends AeronClientOperator {

  private final AeronClientOptions options;

  AeronClientOnOptions(AeronClient source, AeronClientOptions options) {
    super(source);
    this.options = options;
  }

  @Override
  public AeronClientOptions options() {
    return options;
  }
}
