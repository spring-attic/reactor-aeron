package reactor.ipc.aeron;

import org.junit.After;
import org.junit.BeforeClass;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Anatoly Kadyshev
 */
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

    @BeforeClass
    public static void doSetup() {
        AeronTestUtils.setAeronEnvProps();
    }

    @After
    public void doTeardown() {
        disposables.forEach(Disposable::dispose);
        disposables.clear();
    }

}
