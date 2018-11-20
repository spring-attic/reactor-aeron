package io.aeron.driver;

import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

class RetryShutdownTask implements Runnable {

  private static final Logger logger = Loggers.getLogger(RetryShutdownTask.class);

  private final long startMillis;
  private final Scheduler timer;
  private final MonoProcessor<Void> shutdownResult;

  public RetryShutdownTask(Scheduler timer, MonoProcessor<Void> shutdownResult) {
    this.shutdownResult = shutdownResult;
    this.startMillis = System.currentTimeMillis();
    this.timer = timer;
  }

  @Override
  public void run() {
    if (!hasPendingSenderOrSubscriber() || System.currentTimeMillis() - startMillis > shutdownTimeoutNs) {
      forceShutdown();
      shutdownResult.onComplete();
    } else {
      timer.schedule(this, retryShutdownMillis, TimeUnit.MILLISECONDS);
    }
  }

  private boolean hasPendingSenderOrSubscriber() {
    AtomicBoolean canShutdown = new AtomicBoolean(false);
    aeronCounters.forEach(
      (id, label) -> {
        if (label.startsWith(SenderPos.NAME) || label.startsWith(SubscriberPos.NAME)) {
          canShutdown.set(true);
        }
      });
    return canShutdown.get();
  }

  /** NOTE: could result into JVM crashes when there is pending Aeron activity. */
  private void forceShutdown() {
    try {
      aeronCounters.shutdown();
    } catch (Throwable t) {
      logger.error("Failed to shutdown Aeron counters", t);
    }

    CloseHelper.quietClose(driver);

    logger.info("Embedded media driver shutdown complete");
  }
}
