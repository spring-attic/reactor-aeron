package reactor.ipc.aeron;

import io.aeron.Publication;
import reactor.util.Logger;

import java.nio.ByteBuffer;
import java.util.UUID;

public class AeronWriteSequencer extends WriteSequencer<ByteBuffer> {

    private final Logger logger;

    private final Publication publication;

    private final AeronOptions options;

    private final UUID sessionId;

    private final InnerSubscriber<ByteBuffer> inner;

    public AeronWriteSequencer(Logger logger, Publication publication, AeronOptions options, UUID sessionId) {
        super(th -> logger.error("Unexpected exception", th),
                publisher -> {},
                avoid -> false,
                null);
        this.logger = logger;
        this.publication = publication;
        this.options = options;
        this.sessionId = sessionId;
        this.inner = new SignalSender(this, this.publication, this.sessionId, this.options);
    }

    @Override
    InnerSubscriber<ByteBuffer> getInner() {
        return inner;
    }

    class SignalSender extends InnerSubscriber<ByteBuffer> {

        private final Publication publication;

        private final UUID sessionId;

        private final MessagePublisher publisher;

        public SignalSender(AeronWriteSequencer sequencer, Publication publication, UUID sessionId, AeronOptions options) {
            super(sequencer);

            this.publication = publication;
            this.sessionId = sessionId;
            this.publisher = new MessagePublisher(logger, options.connectTimeoutMillis(), options.backpressureTimeoutMillis());

            request(1);
        }

        @Override
        public void doOnNext(ByteBuffer byteBuffer) {
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
        public void doOnError(Throwable t) {
            promise.error(t);
        }

        @Override
        public void doOnComplete() {
            promise.success();
        }

    }

}
