package reactor.aeron;

import io.aeron.driver.MediaDriver.Context;
import java.security.SecureRandom;
import java.util.function.Supplier;

/**
 * Session id generator (in the range {@code 0..Int.MAX_VALUE}) based on {@link SecureRandom}.
 *
 * <p>NOTE: along with this session id generator one must setup {@link
 * Context#publicationReservedSessionIdLow(int)} and {@link
 * Context#publicationReservedSessionIdHigh(int)} of {@link io.aeron.driver.MediaDriver.Context}
 * accordingly.
 */
public final class SecureRandomSessionIdGenerator implements Supplier<Integer> {

  private final SecureRandom random = new SecureRandom(SecureRandom.getSeed(128));

  @Override
  public Integer get() {
    return random.nextInt(Integer.MAX_VALUE);
  }
}
