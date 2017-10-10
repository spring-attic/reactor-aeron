package reactor.ipc.aeron;

import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * @author Anatoly Kadyshev
 */
public final class HeartbeatWatchdog {

    private final Logger logger;

    private final Map<Long, Disposable> disposableBySessionId = new ConcurrentHashMap<>();

    private final Map<Long, Long> lastTimeNsBySessionId = new ConcurrentHashMap<>();

    private final long heartbeatTimeoutMillis;

    private final long timeoutNs;

    public HeartbeatWatchdog(long heartbeatTimeoutMillis, String category) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.logger = Loggers.getLogger(HeartbeatWatchdog.class.getName() + "." + category);
        this.timeoutNs = TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMillis * 3 / 2);
    }

    public void add(long sessionId, Runnable onHeartbeatLostTask, LongSupplier lastSignalTimeNsProvider) {
        lastTimeNsBySessionId.put(sessionId, now());

        AtomicReference<Runnable> taskRef = new AtomicReference<>(() -> {});
        Disposable disposable = Schedulers.single().schedulePeriodically(
                () -> taskRef.get().run(), heartbeatTimeoutMillis * 2, heartbeatTimeoutMillis * 2, TimeUnit.MILLISECONDS);
        disposableBySessionId.put(sessionId, disposable);
        taskRef.set(() -> {
            long lastHeartbeatTimeNs = lastTimeNsBySessionId.get(sessionId);
            long now = now();
            long lastSignalTimeNs = lastSignalTimeNsProvider.getAsLong();
            if ( (now - lastHeartbeatTimeNs > timeoutNs)
                    && (lastSignalTimeNs == 0 || lastSignalTimeNs > 0 && now - lastSignalTimeNs > timeoutNs) ) {
                logger.debug("Lost heartbeat for sessionId: {}", sessionId);
                onHeartbeatLostTask.run();
            }
        });
    }

    private static long now() {
        return System.nanoTime();
    }

    public void remove(long sessionId) {
        Disposable disposable = disposableBySessionId.remove(sessionId);
        if (disposable != null) {
            disposable.dispose();
        }
    }

    public void heartbeatReceived(long sessionId) {
        lastTimeNsBySessionId.computeIfPresent(sessionId, (key, value) -> now());
    }
}
