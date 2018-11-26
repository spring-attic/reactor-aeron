package reactor.aeron;

import static io.aeron.driver.Configuration.IMAGE_LIVENESS_TIMEOUT_NS;
import static io.aeron.driver.Configuration.MTU_LENGTH;

import io.aeron.driver.ThreadingMode;
import java.time.Duration;

public class AeronResourcesConfig {

  public static final boolean DELETE_AERON_DIR_ON_START = true;
  public static final ThreadingMode THREADING_MODE = ThreadingMode.DEDICATED;

  private final ThreadingMode threadingMode;
  private final boolean dirDeleteOnStart;
  private final int mtuLength;
  private final Duration imageLivenessTimeout;

  private AeronResourcesConfig(Builder builder) {
    this.threadingMode = builder.threadingMode;
    this.dirDeleteOnStart = builder.dirDeleteOnStart;
    this.mtuLength = builder.mtuLength;
    this.imageLivenessTimeout = builder.imageLivenessTimeout;
  }

  public static AeronResourcesConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public boolean isDirDeleteOnStart() {
    return dirDeleteOnStart;
  }

  public ThreadingMode threadingMode() {
    return threadingMode;
  }

  public int mtuLength() {
    return mtuLength;
  }

  public Duration imageLivenessTimeout() {
    return imageLivenessTimeout;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AeronResourcesConfig{");
    sb.append(", threadingMode=").append(threadingMode);
    sb.append(", dirDeleteOnStart=").append(dirDeleteOnStart);
    sb.append(", mtuLength=").append(mtuLength);
    sb.append(", imageLivenessTimeout=").append(imageLivenessTimeout);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private ThreadingMode threadingMode = THREADING_MODE;
    private boolean dirDeleteOnStart = DELETE_AERON_DIR_ON_START;
    private int mtuLength = MTU_LENGTH;
    private Duration imageLivenessTimeout = Duration.ofNanos(IMAGE_LIVENESS_TIMEOUT_NS);

    private Builder() {}

    public Builder useThreadModeInvoker() {
      threadingMode = ThreadingMode.INVOKER;
      return this;
    }

    public Builder useThreadModeShared() {
      threadingMode = ThreadingMode.SHARED;
      return this;
    }

    public Builder useThreadModeSharedNetwork() {
      threadingMode = ThreadingMode.SHARED_NETWORK;
      return this;
    }

    public Builder useThreadModeDedicated() {
      threadingMode = ThreadingMode.DEDICATED;
      return this;
    }

    public Builder dirDeleteOnStart(boolean dirDeleteOnStart) {
      this.dirDeleteOnStart = dirDeleteOnStart;
      return this;
    }

    public Builder mtuLength(int mtuLength) {
      this.mtuLength = mtuLength;
      return this;
    }

    public Builder imageLivenessTimeoutNs(Duration imageLivenessTimeout) {
      this.imageLivenessTimeout = imageLivenessTimeout;
      return this;
    }

    public AeronResourcesConfig build() {
      return new AeronResourcesConfig(this);
    }
  }
}
