package reactor.ipc.aeron;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class BaseAeronTest {

  public static final Logger logger = LoggerFactory.getLogger(BaseAeronTest.class);

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  static {
    AeronTestUtils.setAeronEnvProps();
  }

  private final List<Disposable> disposables = new ArrayList<>();

  Disposable blockAndAddDisposable(Mono<? extends Disposable> mono) {
    Disposable disposable = mono.block(TIMEOUT);
    disposables.add(disposable);
    return disposable;
  }

  <T extends Disposable> T addDisposable(T disposable) {
    disposables.add(disposable);
    return disposable;
  }

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    logger.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    try {
      disposables.forEach(Disposable::dispose);
      disposables.clear();
    } finally {
      logger.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
    }
  }
}
