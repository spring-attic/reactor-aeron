package reactor.ipc.aeron.server;

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.PoolerFragmentHandler;
import reactor.ipc.aeron.SignalHandler;
import uk.co.real_logic.aeron.Subscription;

import java.util.Objects;

/**
 * @author Anatoly Kadyshev
 */
class ServerPooler {

    private final Pooler pooler;

    public ServerPooler(Subscription subscription, SignalHandler handler, String name) {
        Objects.requireNonNull(handler, "handler");

        this.pooler = new Pooler(subscription, new PoolerFragmentHandler(handler), name);
    }

    public void initialise() {
        pooler.initialise();
    }

    public Mono<Void> shutdown() {
        return pooler.shutdown();
    }

}
