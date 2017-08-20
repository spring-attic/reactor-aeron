package reactor.ipc.aeron;

import reactor.core.scheduler.Scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public final class RetryTask implements Runnable {

    private final Scheduler scheduler;

    private final long retryMillis;

    private final long timeoutNs;

    private final Callable<Boolean> task;

    private final Runnable onTimeoutTask;

    private long startTimeNs = 0;

    public RetryTask(Scheduler scheduler, long retryMillis, long timeoutMillis,
                     Callable<Boolean> task) {
        this(scheduler, retryMillis, timeoutMillis, task, () -> {});
    }

    RetryTask(Scheduler scheduler, long retryMillis, long timeoutMillis,
              Callable<Boolean> task, Runnable onTimeoutTask) {
        this.scheduler = scheduler;
        this.retryMillis = retryMillis;
        this.timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.task = task;
        this.onTimeoutTask = onTimeoutTask;
    }

    @Override
    public void run() {
        try {
            boolean isCompleted = task.call();
            if (!isCompleted) {
                long now = System.nanoTime();
                if(now - startTimeNs < timeoutNs) {
                    scheduler.schedule(this, retryMillis, TimeUnit.MILLISECONDS);
                } else {
                    onTimeoutTask.run();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void schedule() {
        startTimeNs = System.nanoTime();
        scheduler.schedule(this);
    }

}
