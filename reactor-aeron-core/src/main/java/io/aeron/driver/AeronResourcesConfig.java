package io.aeron.driver;

import java.time.Duration;

public class AeronResourcesConfig {

  public static final boolean DELETE_AERON_DIRS_ON_EXIT = true;
  public static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration RETRY_SHUTDOWN_INTERVAL = Duration.ofMillis(250);

  private final Duration retryShutdownInterval;
  private final Duration shutdownTimeout;
  private final boolean deleteAeronDirsOnExit;

  public AeronResourcesConfig(Builder builder) {
    this.retryShutdownInterval = builder.retryShutdownInterval;
    this.shutdownTimeout = builder.shutdownTimeout;
    this.deleteAeronDirsOnExit = builder.deleteAeronDirsOnExit;
  }

  public static AeronResourcesConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public Duration retryShutdownInterval() {
    return retryShutdownInterval;
  }

  public Duration shutdownTimeout() {
    return shutdownTimeout;
  }

  public boolean isDeleteAeronDirsOnExit() {
    return deleteAeronDirsOnExit;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AeronResourcesConfig{");
    sb.append("retryShutdownInterval=").append(retryShutdownInterval);
    sb.append(", shutdownTimeout=").append(shutdownTimeout);
    sb.append(", deleteAeronDirsOnExit=").append(deleteAeronDirsOnExit);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private Duration retryShutdownInterval = RETRY_SHUTDOWN_INTERVAL;
    private Duration shutdownTimeout = SHUTDOWN_TIMEOUT;
    private boolean deleteAeronDirsOnExit = DELETE_AERON_DIRS_ON_EXIT;

    private Builder() {}

    public Builder retryShutdownInterval(Duration retryShutdownInterval) {
      this.retryShutdownInterval = retryShutdownInterval;
      return this;
    }

    public Builder shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = shutdownTimeout;
      return this;
    }

    public Builder deleteAeronDirsOnExit(boolean deleteAeronDirsOnExit) {
      this.deleteAeronDirsOnExit = deleteAeronDirsOnExit;
      return this;
    }

    public AeronResourcesConfig build() {
      return new AeronResourcesConfig(this);
    }
  }
}
