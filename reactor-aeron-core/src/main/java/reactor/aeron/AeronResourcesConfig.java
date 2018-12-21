package reactor.aeron;

import io.aeron.driver.Configuration;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

public class AeronResourcesConfig {

  private final ThreadingMode threadingMode;
  private final boolean dirDeleteOnStart;
  private final int mtuLength;
  private final Duration imageLivenessTimeout;
  private final int numOfWorkers;
  private final Supplier<IdleStrategy> idleStrategySupplier;

  private AeronResourcesConfig(Builder builder) {
    this.threadingMode = builder.threadingMode;
    this.dirDeleteOnStart = builder.dirDeleteOnStart;
    this.mtuLength = builder.mtuLength;
    this.imageLivenessTimeout = builder.imageLivenessTimeout;
    this.numOfWorkers = builder.numOfWorkers;
    this.idleStrategySupplier = builder.idleStrategySupplier;
  }

  private static BackoffIdleStrategy defaultBackoffIdleStrategy() {
    return new BackoffIdleStrategy(
        100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
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

  public int numOfWorkers() {
    return numOfWorkers;
  }

  public Supplier<IdleStrategy> idleStrategySupplier() {
    return idleStrategySupplier;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AeronResourcesConfig{");
    sb.append(", threadingMode=").append(threadingMode);
    sb.append(", dirDeleteOnStart=").append(dirDeleteOnStart);
    sb.append(", mtuLength=").append(mtuLength);
    sb.append(", imageLivenessTimeout=").append(imageLivenessTimeout);
    sb.append(", numOfWorkers=").append(numOfWorkers);
    sb.append(", idleStrategySupplier=").append(idleStrategySupplier);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private ThreadingMode threadingMode = ThreadingMode.DEDICATED;
    private boolean dirDeleteOnStart = true;
    private int mtuLength = Configuration.MTU_LENGTH;
    private Duration imageLivenessTimeout =
        Duration.ofNanos(Configuration.IMAGE_LIVENESS_TIMEOUT_NS);
    private int numOfWorkers = Runtime.getRuntime().availableProcessors();
    private Supplier<IdleStrategy> idleStrategySupplier =
        AeronResourcesConfig::defaultBackoffIdleStrategy;

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

    public Builder imageLivenessTimeout(Duration imageLivenessTimeout) {
      this.imageLivenessTimeout = imageLivenessTimeout;
      return this;
    }

    public Builder numOfWorkers(int numOfWorkers) {
      this.numOfWorkers = numOfWorkers;
      return this;
    }

    public Builder idleStrategySupplier(Supplier<IdleStrategy> idleStrategySupplier) {
      this.idleStrategySupplier = idleStrategySupplier;
      return this;
    }

    public AeronResourcesConfig build() {
      return new AeronResourcesConfig(this);
    }
  }
}
