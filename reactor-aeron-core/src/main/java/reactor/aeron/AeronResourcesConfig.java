package reactor.aeron;

import io.aeron.CommonContext;
import io.aeron.driver.Configuration;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

public class AeronResourcesConfig {

  private int numOfWorkers = Runtime.getRuntime().availableProcessors();
  private Supplier<IdleStrategy> idleStrategySupplier =
      AeronResourcesConfig::defaultBackoffIdleStrategy;
  private String aeronDirectoryName = generateRandomTmpDirName();

  private static BackoffIdleStrategy defaultBackoffIdleStrategy() {
    return new BackoffIdleStrategy(
        100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
  }

  public static class Builder {

    private Builder() {}

    public Builder useTmpDir() {
      return aeronDirectoryName(generateRandomTmpDirName());
    }

    public Builder useAeronDefaultDir() {
      return aeronDirectoryName(CommonContext.generateRandomDirName());
    }

    public Builder aeronDirectoryName(String dirName) {
      this.aeronDirectoryName = dirName;
      return this;
    }
  }
}
