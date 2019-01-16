package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.AvailableImageHandler;
import io.aeron.UnavailableImageHandler;
import java.time.Duration;
import java.util.function.Consumer;
import org.agrona.ErrorHandler;

public final class AeronResourcesSettings {

  private final Aeron.Context context = new Aeron.Context();

  public AeronResourcesSettings() {}

  AeronResourcesSettings(AeronResourcesSettings other) {
    context
        .aeronDirectoryName(other.context.aeronDirectoryName())
        .availableImageHandler(other.context.availableImageHandler())
        .unavailableImageHandler(other.context.unavailableImageHandler())
        .driverTimeoutMs(other.context.driverTimeoutMs())
        .errorHandler(other.context.errorHandler())
        .keepAliveInterval(other.context.keepAliveInterval())
        .resourceLingerDurationNs(other.context.resourceLingerDurationNs());
  }

  public AeronResourcesSettings aeronDirectoryName(String aeronDirectoryName) {
    return set(c -> c.context.aeronDirectoryName(aeronDirectoryName));
  }

  public AeronResourcesSettings availableImageHandler(AvailableImageHandler imageHandler) {
    return set(c -> c.context.availableImageHandler(imageHandler));
  }

  public AeronResourcesSettings unavailableImageHandler(UnavailableImageHandler imageHandler) {
    return set(c -> c.context.unavailableImageHandler(imageHandler));
  }

  public AeronResourcesSettings driverTimeout(Duration timeout) {
    return set(c -> c.context.driverTimeoutMs(timeout.toMillis()));
  }

  public AeronResourcesSettings errorHandler(ErrorHandler errorHandler) {
    return set(c -> c.context.errorHandler(errorHandler));
  }

  public AeronResourcesSettings keepAliveInterval(Duration interval) {
    return set(c -> c.context.keepAliveInterval(interval.toNanos()));
  }

  public AeronResourcesSettings resourceLinger(Duration duration) {
    return set(c -> c.context.resourceLingerDurationNs(duration.toNanos()));
  }

  private AeronResourcesSettings set(Consumer<AeronResourcesSettings> c) {
    AeronResourcesSettings cfg = new AeronResourcesSettings(this);
    c.accept(cfg);
    return cfg;
  }
}
