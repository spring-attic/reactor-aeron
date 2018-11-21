package reactor.ipc.aeron.server;

import reactor.ipc.aeron.AeronOptions;

class AeronServerOnOptions extends AeronServerOperator {

  private final AeronOptions options;

  AeronServerOnOptions(AeronServer aeronServer, AeronOptions options) {
    super(aeronServer);
    this.options = options;
  }

  @Override
  public AeronOptions options() {
    return options;
  }
}
