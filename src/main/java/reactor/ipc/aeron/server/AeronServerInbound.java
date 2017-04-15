package reactor.ipc.aeron.server;

import reactor.core.publisher.TopicProcessor;
import reactor.ipc.aeron.AeronInbound;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
final class AeronServerInbound implements AeronInbound {

    private final AeronFlux flux;

    private final TopicProcessor<ByteBuffer> processor;

    public AeronServerInbound(String name) {
        //FIXME: Use different processor
        this.processor = TopicProcessor.create(name);
        this.flux = new AeronFlux(processor);
    }

    @Override
    public AeronFlux receive() {
        return flux;
    }

    public void onNext(ByteBuffer buffer) {
        processor.onNext(buffer);
    }

}
