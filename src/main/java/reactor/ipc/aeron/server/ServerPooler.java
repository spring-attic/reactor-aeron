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
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.MessageHandler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * @author Anatoly Kadyshev
 */
final class ServerPooler implements MessageHandler {

    private final Map<UUID, SessionData> inboundBySessionId;

    private final String category;

    private final AeronWrapper wrapper;

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

    private final AeronOptions options;

    //FIXME: Get rid of
    private final Pooler pooler;

    private final Logger logger;

    static int serverStreamId = 1;

    ServerPooler(String category,
                 AeronWrapper wrapper,
                 BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                 AeronOptions options,
                 Subscription subscription) {
        this.category = category;
        this.wrapper = wrapper;
        this.ioHandler = ioHandler;
        this.options = options;
        this.inboundBySessionId = new ConcurrentHashMap<>();
        this.logger = Loggers.getLogger(AeronServer.class + "." + category);
        this.pooler = new Pooler(category);
        pooler.addSubscription(subscription, this);
    }

    public void initialise() {
        pooler.initialise();
    }

    public Mono<Void> shutdown() {
        return pooler.shutdown();
    }

    @Override
    public void onConnect(UUID sessionId, String clientChannel, int clientStreamId, int clientAckStreamId) {
        logger.debug("Received CONNECT for sessionId: {}, channel/streamId: {}/{}", sessionId, clientChannel, clientStreamId);

        SessionData sessionData = new SessionData(clientChannel, clientStreamId, clientAckStreamId, sessionId);
        sessionData.initialise();
    }

    @Override
    public void onNext(UUID sessionId, ByteBuffer buffer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Received NEXT for sessionId: {}, buffer: {}", sessionId, buffer);
        }

        SessionData sessionData = inboundBySessionId.get(sessionId);
        if (sessionData != null) {
            sessionData.inbound.onNext(buffer);
        } else {
            //FIXME: Add additional handling
            logger.error("Could not find session with sessionId: {}", sessionId);
        }
    }

    @Override
    public void onConnectAck(UUID sessionId, int serverStreamId) {
        throw new UnsupportedOperationException();
    }

    class SessionData implements Disposable {

        final AeronOutbound outbound;

        final AeronServerInbound inbound;

        private final String channel;

        private final int streamId;

        private final int clientAckStreamId;

        private final UUID sessionId;

        SessionData(String channel, int streamId, int clientAckStreamId, UUID sessionId) {
            this.channel = channel;
            this.streamId = streamId;
            this.clientAckStreamId = clientAckStreamId;
            this.sessionId = sessionId;
            this.outbound = new AeronOutbound(category, wrapper, channel, sessionId, options);
            this.inbound = new AeronServerInbound(category);
        }

        void initialise() {
            inboundBySessionId.put(sessionId, this);

            outbound.initialise(streamId)
                    .then(sendConnectAck())
                    .doOnSuccess(avoid -> {
                        Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
                        Mono.from(publisher).doOnTerminate((avoid2, th) -> {
                            dispose();
                            logger.debug("Closed session with sessionId: {}", sessionId);
                        }).subscribe();
                    })
                    .doOnError(th -> {
                        dispose();
                        logger.debug("Failed to connect to the client for sessionId: {}", sessionId);
                    })
                    .subscribe();
        }

        private Mono<Void> sendConnectAck() {
            return Mono.create(sink -> {
                Publication publication = wrapper.addPublication(channel, clientAckStreamId, "sending acks", sessionId);
                MessagePublisher publisher = new MessagePublisher(logger, options.connectTimeoutMillis(),
                        options.backpressureTimeoutMillis());

                int serverDedicatedStreamId = options.serverStreamId() + serverStreamId++;
                //FIXME: Exceptions handling
                long result = publisher.publish(publication,
                        MessageType.CONNECT_ACK,
                        Protocol.createConnectAckBody(serverDedicatedStreamId), sessionId);

                if (result > 0) {
                    pooler.addSubscription(wrapper.addSubscription(options.serverChannel(),
                            serverDedicatedStreamId, "server pooler", sessionId), ServerPooler.this);

                    sink.success();
                } else {
                    sink.error(new Exception("Failed to send " + MessageType.CONNECT_ACK));
                }
                publication.close();
            });
        }

        @Override
        public void dispose() {
            inboundBySessionId.remove(sessionId);

            outbound.dispose();
            inbound.dispose();
        }

    }

}
