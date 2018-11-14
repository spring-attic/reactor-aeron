package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronCounters;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Driver manager. */
public final class DriverManager {

  private static final Logger logger = Loggers.getLogger(DriverManager.class);

  private final State notStartedState = new NotStartedState();

  // TODO: Make configurable
  private final int retryShutdownMillis = 250;

  private final long shutdownTimeoutNs = TimeUnit.SECONDS.toNanos(5);

  // TODO: Make configurable
  private final boolean deleteAeronDirsOnExit = true;

  private Thread shutdownHook;

  private State state = notStartedState;

  private MediaDriver driver;
  private MediaDriver.Context mediaContext;

  private Aeron aeron;
  private Aeron.Context aeronContext;

  private AeronCounters aeronCounters;

  private final List<String> aeronDirNames = Collections.synchronizedList(new ArrayList<>());

  private interface State {

    default void launch() {}

    Mono<Void> shutdown();
  }

  private class NotStartedState implements State {

    @Override
    public void launch() {
      doInitialize();

      setState(new StartedState());
    }

    @Override
    public Mono<Void> shutdown() {
      return Mono.empty();
    }
  }

  private class StartedState implements State {

    int counter = 1;

    @Override
    public void launch() {
      counter++;
    }

    @Override
    public Mono<Void> shutdown() {
      if (--counter == 0) {
        Mono<Void> shutdownResult = doShutdown();

        setState(new ShuttingDownState(shutdownResult));
        return shutdownResult;
      }
      return Mono.empty();
    }
  }

  private class ShuttingDownState implements State {

    private final Mono<Void> shutdownResult;

    ShuttingDownState(Mono<Void> shutdownResult) {
      this.shutdownResult = shutdownResult;
    }

    @Override
    public void launch() {}

    @Override
    public Mono<Void> shutdown() {
      return shutdownResult;
    }
  }

  public synchronized void launchDriver() {
    state.launch();
  }

  public synchronized Mono<Void> shutdownDriver() {
    return state.shutdown();
  }

  private void doInitialize() {
    mediaContext = new MediaDriver.Context();
    driver = MediaDriver.launchEmbedded(mediaContext);

    aeronContext = new Aeron.Context();
    String aeronDirName = driver.aeronDirectoryName();
    aeronContext.aeronDirectoryName(aeronDirName);

    aeron = Aeron.connect(aeronContext);
    aeronCounters = new AeronCounters(aeronDirName);
    aeronDirNames.add(aeronDirName);

    setupShutdownHook();

    logger.info("Embedded media driver initialized, aeronDirName: {}", aeronDirName);
  }

  private Mono<Void> doShutdown() {
    logger.info("Embedded media driver shutdown initiated");

    aeron.close();

    MonoProcessor<Void> shutdownResult = MonoProcessor.create();
    Scheduler timer = Schedulers.single();
    timer.schedule(
        new RetryShutdownTask(timer, shutdownResult), retryShutdownMillis, TimeUnit.MILLISECONDS);
    return shutdownResult;
  }

  private void setupShutdownHook() {
    if (!deleteAeronDirsOnExit || shutdownHook != null) {
      return;
    }

    shutdownHook =
        new Thread(
            () ->
                aeronDirNames.forEach(
                    dir -> {
                      try {
                        File dirFile = new File(dir);
                        IoUtil.delete(dirFile, false);
                      } catch (Exception e) {
                        logger.error("Failed to delete Aeron directory: {}", dir);
                      }
                    }));

    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /** NOTE: could result into JVM crashes when there is pending Aeron activity. */
  private synchronized void forceShutdown() {
    aeron = null;
    try {
      aeronCounters.shutdown();
    } catch (Throwable t) {
      logger.error("Failed to shutdown Aeron counters", t);
    }
    aeronCounters = null;

    CloseHelper.quietClose(driver);

    driver = null;

    setState(notStartedState);

    logger.info("Embedded media driver shutdown complete");
  }

  private void setState(State nextState) {
    state = nextState;
  }

  public synchronized AeronCounters getAeronCounters() {
    return aeronCounters;
  }

  public synchronized Aeron getAeron() {
    return aeron;
  }

  public Context getMediaContext() {
    return mediaContext;
  }

  public Aeron.Context getAeronContext() {
    return aeronContext;
  }

  private class RetryShutdownTask implements Runnable {

    private final long startNs;

    private final Scheduler timer;

    private final MonoProcessor<Void> shutdownResult;

    public RetryShutdownTask(Scheduler timer, MonoProcessor<Void> shutdownResult) {
      this.shutdownResult = shutdownResult;
      this.startNs = System.nanoTime();
      this.timer = timer;
    }

    @Override
    public void run() {
      if (!hasPendingSenderOrSubscriber() || System.nanoTime() - startNs > shutdownTimeoutNs) {
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
  }
}
