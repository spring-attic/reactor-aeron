package reactor.ipc.aeron;

import io.aeron.Publication;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

final class AeronWriteSequencer extends WriteSequencer<ByteBuffer> {

    private static final Logger logger = Loggers.getLogger(AeronWriteSequencer.class);

    private final String category;

    private final Publication publication;

    private final AeronOptions options;

    private final long sessionId;

    private final InnerSubscriber<ByteBuffer> inner;

    private final Consumer<Throwable> errorHandler;

    AeronWriteSequencer(String category, Publication publication, AeronOptions options, long sessionId) {
        super(publisher -> {}, avoid -> false,null);
        this.category = category;
        this.publication = publication;
        this.options = options;
        this.sessionId = sessionId;
        this.errorHandler = th -> logger.error("[{}] Unexpected exception", category, th);
        this.inner = new SignalSender(this, this.publication, this.sessionId, this.options);
    }

    @Override
    Consumer<Throwable> getErrorHandler() {
        return errorHandler;
    }

    @Override
    InnerSubscriber<ByteBuffer> getInner() {
        return inner;
    }

    class SignalSender extends InnerSubscriber<ByteBuffer> {

        private final Publication publication;

        private final long sessionId;

        private final MessagePublisher publisher;

        SignalSender(AeronWriteSequencer sequencer, Publication publication, long sessionId, AeronOptions options) {
            super(sequencer);

            this.publication = publication;
            this.sessionId = sessionId;
            this.publisher = new MessagePublisher(category, options.connectTimeoutMillis(), options.backpressureTimeoutMillis());

            request(1);
        }

        @Override
        void doOnNext(ByteBuffer byteBuffer) {
            Exception cause = null;
            long result = 0;
            try {
                result = publisher.publish(publication, MessageType.NEXT, byteBuffer, sessionId);
                if (result > 0) {
                    request(1);
                    return;
                }
            } catch (Exception ex) {
                cancel();

                cause = ex;
            }
            promise.error(new Exception("Failed to publish signal into session with Id: " + sessionId
                    + ", result=" + result, cause));
        }

        @Override
        void doOnError(Throwable t) {
            promise.error(t);
        }

        @Override
        void doOnComplete() {
            promise.success();
        }

    }

}
