/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.ipc.aeron;

import io.aeron.Publication;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.publisher.WriteSequencer;
import reactor.ipc.aeron.publisher.WriteSequencerSubscription;
import reactor.ipc.connector.Outbound;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronOutbound implements Outbound<ByteBuffer>, Disposable {

    private final Logger logger;

    private final SignalSender signalSender;

    private final WriteSequencer<ByteBuffer> sequencer;

    private final Scheduler scheduler;

    public AeronOutbound(String category,
                         AeronWrapper wrapper,
                         String channel,
                         int streamId,
                         UUID sessionId,
                         AeronOptions options) {
        Publication publication = wrapper.addPublication(channel, streamId, "sending data", sessionId);

        this.logger = Loggers.getLogger(AeronOutbound.class + "." + category);
        this.signalSender = new SignalSender(publication, sessionId, options);
        this.sequencer = new WriteSequencer<>(signalSender,
                th -> logger.error("Unexpected exception", th),
                publisher -> {},
                avoid -> false,
                null);

        this.scheduler = Schedulers.newParallel("aeron-sender", 1);
    }

    @Override
    public Outbound<ByteBuffer> send(Publisher<? extends ByteBuffer> dataStream) {
        return then(sequencer.add(dataStream, scheduler));
    }

    @Override
    public void dispose() {
        scheduler.dispose();
    }

    class SignalSender implements Subscriber<ByteBuffer> {

        private final Publication publication;

        private WriteSequencerSubscription s;

        private final UUID sessionId;

        private final MessagePublisher publisher;

        public SignalSender(Publication publication, UUID sessionId, AeronOptions options) {
            this.publication = publication;
            this.sessionId = sessionId;
            this.publisher = new MessagePublisher(logger, options.connectTimeoutMillis(), options.backpressureTimeoutMillis());
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = (WriteSequencerSubscription) s;

            s.request(1);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            long result = 0;
            Exception cause = null;
            try {
                result = publisher.publish(publication, MessageType.NEXT, byteBuffer, sessionId);
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
