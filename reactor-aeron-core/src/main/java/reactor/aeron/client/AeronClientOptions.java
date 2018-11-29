package reactor.aeron.client;

import io.aeron.driver.Configuration;
import java.time.Duration;
import java.util.Objects;
import reactor.aeron.AeronOptions;

public final class AeronClientOptions extends AeronOptions {

  private String clientChannel = "aeron:udp?endpoint=localhost:12001";

  private Duration ackTimeout = Duration.ofSeconds(10);

  private int mtuLength = Configuration.MTU_LENGTH;

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

  public int mtuLength() {
    return mtuLength;
  }

  public void mtuLength(int mtuLength) {
    this.mtuLength = mtuLength;
  }
}
