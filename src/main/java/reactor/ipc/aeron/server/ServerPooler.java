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
package reactor.ipc.aeron.server;

import io.aeron.Publication;
import io.aeron.Subscription;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.MessageHandler;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.Protocol;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * @author Anatoly Kadyshev
 */
final class ServerPooler implements MessageHandler {

    private final String category;

    private final AeronWrapper wrapper;

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

    private final AeronOptions options;

    private final Pooler pooler;

    private final Logger logger = Loggers.getLogger(ServerPooler.class.getName());

    private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

    private final AtomicLong nextSessionId = new AtomicLong(0);

    ServerPooler(String category,
                 AeronWrapper wrapper,
                 BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                 AeronOptions options,
                 Subscription subscription) {
        this.category = category;
        this.wrapper = wrapper;
        this.ioHandler = ioHandler;
        this.options = options;
        Pooler pooler = new Pooler(category);
        pooler.addSubscription(subscription, this);
        this.pooler = pooler;
    }

    public void initialise() {
        pooler.initialise();
    }

    public Mono<Void> shutdown() {
        return pooler.shutdown();
    }

    @Override
    public void onConnect(UUID connectRequestId, String clientChannel, int clientControlStreamId, int clientSessionStreamId) {
        logger.debug("Received {} for connectRequestId: {}, channel={}, clientControlStreamId={}, clientSessionStreamId={}",
                MessageType.CONNECT, connectRequestId, AeronUtils.minifyChannel(clientChannel),
                clientControlStreamId, clientSessionStreamId);

        int serverSessionStreamId = streamIdCounter.incrementAndGet();
        long sessionId = nextSessionId.incrementAndGet();
        SessionHandler sessionHandler = new SessionHandler(clientChannel, clientSessionStreamId, clientControlStreamId,
                connectRequestId, sessionId, serverSessionStreamId);
        sessionHandler.initialise();
    }

    @Override
    public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
        throw new UnsupportedOperationException();
    }

    class SessionHandler implements Disposable, MessageHandler {

        private final Logger logger = Loggers.getLogger(SessionHandler.class.getName());

        private final AeronOutbound outbound;

        private final AeronServerInbound inbound;

        private final String clientChannel;

        private final int clientSessionStreamId;

        private final int clientControlStreamId;

        private final UUID connectRequestId;

        private final long sessionId;

        private final int serverSessionStreamId;

        private volatile Subscription dataSubscription;

        SessionHandler(String clientChannel, int clientSessionStreamId, int clientControlStreamId,
                       UUID connectRequestId, long sessionId, int serverSessionStreamId) {
            this.clientChannel = clientChannel;
            this.clientSessionStreamId = clientSessionStreamId;
            this.clientControlStreamId = clientControlStreamId;
            this.outbound = new AeronOutbound(category, wrapper, clientChannel, options);
            this.connectRequestId = connectRequestId;
            this.sessionId = sessionId;
            this.inbound = new AeronServerInbound(category);
            this.serverSessionStreamId = serverSessionStreamId;
        }

        void initialise() {
            sendConnectAck()
                    .doOnSuccess(avoid -> {
                        dataSubscription = wrapper.addSubscription(options.serverChannel(),
                                serverSessionStreamId, "client data", sessionId);
                        pooler.addSubscription(dataSubscription, this);
                    })
                    .then(outbound.initialise(sessionId, clientSessionStreamId))
                    .doOnSuccess(avoid -> {
                        Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
                        Mono.from(publisher).doOnTerminate((avoid2, th) -> {
                            dispose();
                            logger.debug("Closed session with sessionId: {}", sessionId);
                        }).subscribe();
                    })
                    .doOnError(th -> {
                        dispose();
                        logger.debug("Failed to connect to the client for sessionId: {}", sessionId, th);
                    })
                    .subscribe();
        }

        private Mono<Void> sendConnectAck() {
            return Mono.create(sink -> {
                Publication publication = wrapper.addPublication(clientChannel, clientControlStreamId,
                        "sending " + MessageType.CONNECT_ACK, sessionId);
                MessagePublisher publisher = new MessagePublisher(logger, options.connectTimeoutMillis(),
                        options.backpressureTimeoutMillis());

                //FIXME: Exceptions handling
                long result = publisher.publish(publication,
                        MessageType.CONNECT_ACK,
                        Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId), sessionId);

                if (result > 0) {
                    logger.debug("Sent {} to {}", MessageType.CONNECT_ACK, AeronUtils.format(publication));
                    sink.success();
                } else {
                    sink.error(new Exception("Failed to send " + MessageType.CONNECT_ACK));
                }

                publication.close();
            });
        }

        @Override
        public void onNext(long sessionId, ByteBuffer buffer) {
            if (logger.isDebugEnabled()) {
                logger.debug("Received {} for sessionId: {}, buffer: {}", MessageType.NEXT, sessionId, buffer);
            }

            if (this.sessionId == sessionId) {
                inbound.onNext(buffer);
            } else {
                logger.error("Received {} for unexpected sessionId: {}", MessageType.NEXT, sessionId);
            }
        }

        @Override
        public void dispose() {
            if (dataSubscription != null) {
                pooler.removeSubscription(dataSubscription);
                dataSubscription.close();
            }
            outbound.dispose();
            inbound.dispose();
        }

    }

}
