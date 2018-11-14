package reactor.ipc.aeron;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.IdleStrategy;

/** Condition. */
public class Condition {

  private final BooleanSupplier predicate;

  /**
   * Constructor.
   *
   * @param predicate predicate
   */
  public Condition(BooleanSupplier predicate) {
    Objects.requireNonNull(predicate, "predicate shouldn't be null");

    this.predicate = predicate;
  }

  public void awaitTrue(Duration timeout) {
    IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
    long start = System.nanoTime();
    for (; ; ) {
      if (predicate.getAsBoolean()) {
        break;
      }

      if (Duration.ofNanos(System.nanoTime() - start).compareTo(timeout) > 0) {
        throw new RuntimeException(
            String.format("Condition was false during %d millis", timeout.toMillis()));
      }

      idleStrategy.idle(0);
    }
  }
}
