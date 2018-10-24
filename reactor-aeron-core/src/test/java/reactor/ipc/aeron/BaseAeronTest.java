package reactor.ipc.aeron;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class BaseAeronTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  private final List<Disposable> disposables = new ArrayList<>();

  Disposable blockAndAddDisposable(Mono<? extends Disposable> mono) {
    Disposable disposable = mono.block(TIMEOUT);
    disposables.add(disposable);
    return disposable;
  }

  Disposable addDisposable(Disposable disposable) {
    disposables.add(disposable);
    return disposable;
  }

  /** Setup. */
  @BeforeAll
  public static void doSetup() {
    AeronTestUtils.setAeronEnvProps();
  }

  /** Teardown. */
  @AfterEach
  public void doTeardown() {
    disposables.forEach(Disposable::dispose);
    disposables.clear();
  }
}
