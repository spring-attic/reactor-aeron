package reactor.ipc.aeron;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.publisher.MergePublisher;
import reactor.ipc.aeron.publisher.MergePublisherSubscription;
import reactor.ipc.connector.Outbound;
import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronOutbound implements Outbound<ByteBuffer> {

    private final Logger logger;

    private final SignalSender signalSender;

    private final MergePublisher<ByteBuffer> publisher;

    private final Scheduler scheduler;

    public AeronOutbound(String category,
                         AeronHelper aeronHelper,
                         String channel,
                         int streamId,
                         UUID sessionId,
                         AeronOptions options) {
        Publication publication = aeronHelper.addPublication(channel, streamId, "sending data", sessionId);

        this.logger = Loggers.getLogger(AeronOutbound.class + "." + category);
        this.signalSender = new SignalSender(publication, sessionId, options);
        this.publisher = new MergePublisher<>(signalSender,
                th -> logger.error("Unexpected exception", th),
                publisher -> {},
                avoid -> false,
                null);

        this.scheduler = Schedulers.single();
    }

    @Override
    public Outbound<ByteBuffer> send(Publisher<? extends ByteBuffer> dataStream) {
        Outbound<ByteBuffer> result = then(publisher.add(dataStream));
        scheduler.schedule(publisher::drain);
        return result;
    }

    class SignalSender implements Subscriber<ByteBuffer> {

        private final Publication publication;

        private MergePublisherSubscription s;

        private final UUID sessionId;

        private final AeronOptions options;

        private final IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

        public SignalSender(Publication publication, UUID sessionId, AeronOptions options) {
            this.publication = publication;
            this.sessionId = sessionId;
            this.options = options;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = (MergePublisherSubscription) s;

            s.request(1);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            long result = 0;
            Exception cause = null;
            try {
                result = AeronUtils.publish(logger, publication, RequestType.NEXT, byteBuffer, idleStrategy, sessionId,
                        options.connectTimeoutMillis(), options.backpressureTimeoutMillis());
            } catch (Exception e) {
                cause = e;
            }
            if (result > 0) {
                s.request(1);
            } else {
                s.cancel();

                String message = "Failed to publish signal into session with Id: " + sessionId;
                s.getPromise().error(cause == null ? new Exception(message): new Exception(message, cause));
            }
        }

        @Override
        public void onError(Throwable t) {
            s.getPromise().error(t);
        }

        @Override
        public void onComplete() {
            s.getPromise().success();
        }

    }

}
