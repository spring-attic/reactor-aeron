package reactor.aeron;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Wrapper around the {@link AeronEventLoop} where the actual logic is performed. Manages grouping
 * of multiple instances of {@link AeronEventLoop}: round-robin iteration and grouped disposal.
 */
public class AeronEventLoopGroup implements OnDisposable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AeronEventLoopGroup.class);

  // State
  private final AeronEventLoop[] eventLoops;
  private final AtomicInteger idx = new AtomicInteger();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructs an instance of {@link AeronEventLoopGroup}.
   *
   * @param idleStrategy idle strategy to follow between work cycles
   * @param workers number of instances in group
   */
  public AeronEventLoopGroup(Supplier<IdleStrategy> idleStrategy, int workers) {
    this.eventLoops = new AeronEventLoop[workers];
    String parentId = Integer.toHexString(System.identityHashCode(this));
    for (int i = 0; i < workers; i++) {
      eventLoops[i] = new AeronEventLoop(i, parentId, idleStrategy.get());
    }
    // Setup shutdown
    Mono.whenDelayError(
            Arrays.stream(eventLoops) //
                .map(AeronEventLoop::onDispose)
                .toArray(Mono<?>[]::new))
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, ex -> LOGGER.error("Unexpected exception occurred: " + ex));
  }

  /**
   * Get instance of worker from the group (round-robin iteration).
   *
   * @return instance of worker in the group
   */
  public AeronEventLoop next() {
    return eventLoops[Math.abs(idx.getAndIncrement() % eventLoops.length)];
  }

  /**
   * Completes successfully once all the workers in the group are disposed.
   *
   * @return mono that completes once workers are disposed
   */
  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  /** Dispose all the workers in group. */
  @Override
  public void dispose() {
    if (!isDisposed()) {
      Arrays.stream(eventLoops).forEach(AeronEventLoop::dispose);
    }
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }
}
