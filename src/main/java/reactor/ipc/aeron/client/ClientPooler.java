package reactor.ipc.aeron.client;

import reactor.core.publisher.FluxSink;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.PoolerFragmentHandler;
import reactor.ipc.aeron.SignalHandler;
import uk.co.real_logic.aeron.Subscription;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
final class ClientPooler implements SignalHandler {

    private final Pooler pooler;

    private final UUID sessionId;

    private FluxSink<ByteBuffer> fluxSink;

    public void setFluxSink(FluxSink<ByteBuffer> fluxSink) {
        this.fluxSink = fluxSink;
    }

    public ClientPooler(Subscription aeronSub, UUID sessionId, String name) {
        Objects.requireNonNull(aeronSub, "aeronSub");
        Objects.requireNonNull(sessionId, "sessionId");


        this.pooler = new Pooler(aeronSub, new PoolerFragmentHandler(this), name);
        this.sessionId = sessionId;
    }

    public void initialise() {
        pooler.initialise();
    }

    public void shutdown() {
        pooler.shutdown();
    }

    @Override
    public void onConnect(UUID sessionId, String channel, int streamId) {
        throw new UnsupportedOperationException("Client doesn't support CONNECT requests");
    }

    @Override
    public void onNext(UUID sessionId, ByteBuffer buffer) {
        if (!sessionId.equals(this.sessionId)) {
            throw new RuntimeException("Received session for unknown sessionId: " + sessionId);
        }
        fluxSink.next(buffer);
    }

}
