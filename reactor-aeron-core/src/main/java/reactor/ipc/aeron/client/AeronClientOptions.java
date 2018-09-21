package reactor.ipc.aeron.client;

import java.time.Duration;
import java.util.Objects;
import reactor.ipc.aeron.AeronOptions;

public final class AeronClientOptions extends AeronOptions {

  private String clientChannel = "aeron:udp?endpoint=localhost:12001";

  private Duration ackTimeout = Duration.ofSeconds(10);

  public String clientChannel() {
    return clientChannel;
  }

  public void clientChannel(String clientChannel) {
    this.clientChannel = Objects.requireNonNull(clientChannel, "clientChannel");
  }

  public Duration ackTimeout() {
    return ackTimeout;
  }

  public void ackTimeout(Duration ackTimeout) {
    this.ackTimeout = Objects.requireNonNull(ackTimeout, "ackTimeout");
  }
}
