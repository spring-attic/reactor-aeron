package reactor.ipc.aeron.server;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSource;
import reactor.ipc.aeron.AeronUtils;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronFlux extends FluxSource<ByteBuffer, ByteBuffer> {

    public AeronFlux(Publisher<? extends ByteBuffer> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        source.subscribe(s);
    }

    public Flux<String> asString() {
        return map(AeronUtils::byteBufferToString);
    }
}
