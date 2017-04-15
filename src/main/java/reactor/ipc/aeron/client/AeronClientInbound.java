package reactor.ipc.aeron.client;

import reactor.core.publisher.FluxProcessor;
import reactor.ipc.aeron.AeronHelper;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.server.AeronFlux;
import uk.co.real_logic.aeron.Subscription;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
final class AeronClientInbound implements AeronInbound {

    private final ClientPooler pooler;

    private final AeronFlux flux;

    public AeronClientInbound(AeronHelper aeronHelper, String channel, int streamId, UUID sessionId, String name) {
        Objects.requireNonNull(sessionId, "sessionId");

        Subscription subscription = aeronHelper.addSubscription(channel, streamId, "receiving data", sessionId);

        this.pooler = new ClientPooler(subscription, sessionId, name);
        this.flux = new AeronFlux(FluxProcessor.create(emitter -> {
            pooler.setFluxSink(emitter);
            pooler.initialise();
        }));
    }

    @Override
    public AeronFlux receive() {
        return flux;
    }

    public void shutdown() {
        pooler.shutdown();
    }

}
