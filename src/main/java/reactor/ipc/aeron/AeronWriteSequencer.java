package reactor.ipc.aeron;

import io.aeron.Publication;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

final class AeronWriteSequencer extends WriteSequencer<ByteBuffer> {

    private final Logger logger;

    private final Publication publication;

    private final AeronOptions options;

    private final long sessionId;

    private final InnerSubscriber<ByteBuffer> inner;

    private final Consumer<Throwable> errorHandler;

    AeronWriteSequencer(String category, Publication publication, AeronOptions options, long sessionId) {
        super(publisher -> {}, avoid -> false,null);
        this.publication = publication;
        this.options = options;
        this.sessionId = sessionId;
        this.inner = new SignalSender(this, this.publication, this.sessionId, this.options);
        this.logger = Loggers.getLogger(AeronWriteSequencer.class + "." + category);
        this.errorHandler = th -> logger.error("Unexpected exception", th);
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
            this.publisher = new MessagePublisher(logger, options.connectTimeoutMillis(), options.backpressureTimeoutMillis());

            request(1);
        }

        @Override
        void doOnNext(ByteBuffer byteBuffer) {
            long result = 0;
            Exception cause = null;
            try {
                result = publisher.publish(publication, MessageType.NEXT, byteBuffer, sessionId);
            } catch (Exception e) {
                cause = e;
            }
            if (result > 0) {
                request(1);
            } else {
                cancel();

                String message = "Failed to publish signal into session with Id: " + sessionId;
                promise.error(cause == null ? new Exception(message): new Exception(message, cause));
            }
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
