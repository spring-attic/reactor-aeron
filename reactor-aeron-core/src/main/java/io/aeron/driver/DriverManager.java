package io.aeron.driver;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class DriverManager {

  private static final Logger logger = Loggers.getLogger(DriverManager.class);

  private Mono<Void> doShutdown() {
    logger.info("Embedded media driver shutdown initiated");

    aeron.close();

    retry = Retry.anyOf(IOException.class)
      .randomBackoff(Duration.ofMillis(100), Duration.ofSeconds(60))
      .doOnRetry(context -> context.applicationContext().rollback());
    flux.retryWhen(retry);


    MonoProcessor<Void> shutdownResult = MonoProcessor.create();
    Scheduler timer = Schedulers.single();
    timer.schedule(
        new RetryShutdownTask(timer, shutdownResult), retryShutdownMillis, TimeUnit.MILLISECONDS);
    return shutdownResult;
  }
}
