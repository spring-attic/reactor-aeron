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
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.MessageHandler;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.RetryTask;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * @author Anatoly Kadyshev
 */
final class ServerPooler implements MessageHandler {

    private static final Logger logger = Loggers.getLogger(ServerPooler.class);

    private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

    public static final Disposable NO_OP = () -> {};

    private final String category;

    private final AeronWrapper wrapper;

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

    private final AeronOptions options;

    private final Pooler pooler;

    private final AtomicLong nextSessionId = new AtomicLong(0);

    private final HeartbeatWatchdog heartbeatWatchdog;

    private final Map<Long, SessionHandler> sessionHandlerById = new ConcurrentHashMap<>();

    private final HeartbeatSender heartbeatSender;

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
        this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), category);
        this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), category);
    }

    public void initialise() {
        pooler.initialise();
    }

    public Mono<Void> shutdown() {
        return pooler.shutdown()
                .doOnTerminate((avoid, th) -> sessionHandlerById.values().forEach(SessionHandler::dispose));
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

    //FIXME: Remove
    @Override
    public void onDisonnect(long sessionId) {
        SessionHandler sessionHandler = sessionHandlerById.get(sessionId);
        if (sessionHandler != null) {
            sessionHandler.dispose();
        }
    }

    @Override
    public void onHeartbeat(long sessionId) {
        heartbeatWatchdog.heartbeatReceived(sessionId);
    }

    class SessionHandler implements Disposable, MessageHandler {

        private final Logger logger = Loggers.getLogger(SessionHandler.class);

        private final AeronOutbound outbound;

        private final AeronServerInbound inbound;

        private final int clientSessionStreamId;

        private final UUID connectRequestId;

        private final long sessionId;

        private final int serverSessionStreamId;

        private final Publication clientControlPub;

        private final Subscription serverDataSub;

        private volatile Disposable heartbeatDisposable = NO_OP;

        SessionHandler(String clientChannel, int clientSessionStreamId, int clientControlStreamId,
                       UUID connectRequestId, long sessionId, int serverSessionStreamId) {
            this.clientSessionStreamId = clientSessionStreamId;
            this.outbound = new AeronOutbound(category, wrapper, clientChannel, options);
            this.connectRequestId = connectRequestId;
            this.sessionId = sessionId;
            this.inbound = new AeronServerInbound(category);
            this.serverSessionStreamId = serverSessionStreamId;
            this.clientControlPub = wrapper.addPublication(clientChannel, clientControlStreamId,
                    "sending " + MessageType.CONNECT_ACK, sessionId);
            this.serverDataSub = wrapper.addSubscription(options.serverChannel(),
                    serverSessionStreamId, "client data", sessionId);
        }

        void initialise() {
            pooler.addSubscription(serverDataSub, this);

            Mono<Void> initialiseOutbound = outbound.initialise(sessionId, clientSessionStreamId);

            sendConnectAck()
                    .then(initialiseOutbound)
                    .doOnSuccess(avoid -> {

                        heartbeatWatchdog.add(sessionId, () -> {
                                heartbeatWatchdog.remove(sessionId);
                                dispose();
                            },
                                ignore -> inbound.getLastSignalTimeNs());

                        heartbeatDisposable = heartbeatSender.scheduleHeartbeats(clientControlPub, sessionId)
                                .doOnError(th -> heartbeatDisposable.dispose())
                                .subscribe();

                        sessionHandlerById.put(sessionId, this);

                        Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
                        Mono.from(publisher).doOnTerminate((avoid2, th) -> {
                            dispose();
                        }).subscribe();
                    })
                    .doOnError(th -> {
                        dispose();
                        logger.debug("Failed to connect to the client for sessionId: {}", sessionId, th);
                    })
                    .subscribe();
        }

        private Mono<Void> sendConnectAck() {
            return Mono.create(sink ->
                    new RetryTask(Schedulers.single(), 100,
                            options.connectTimeoutMillis() + options.backpressureTimeoutMillis(),
                            new SendConnectAckTask(sink)).schedule());
        }

        @Override
        public void onNext(long sessionId, ByteBuffer buffer) {
            if (logger.isTraceEnabled()) {
                logger.trace("Received {} for sessionId: {}, buffer: {}", MessageType.NEXT, sessionId, buffer);
            }

            if (this.sessionId == sessionId) {
                inbound.onNext(buffer);
            } else {
                logger.error("Received {} for unexpected sessionId: {}", MessageType.NEXT, sessionId);
            }
        }

        @Override
        public void onComplete(long sessionId) {
            if (logger.isTraceEnabled()) {
                logger.trace("Received {} for sessionId: {}", MessageType.COMPLETE, sessionId);
            }

            SessionHandler sessionHandler = sessionHandlerById.get(sessionId);
            if (sessionHandler != null) {
                sessionHandler.dispose();
            }
        }

        @Override
        public void dispose() {
            sessionHandlerById.remove(this);

            pooler.removeSubscription(serverDataSub);
            serverDataSub.close();

            heartbeatDisposable.dispose();
            heartbeatWatchdog.remove(sessionId);

            clientControlPub.close();

            outbound.dispose();
            inbound.dispose();

            logger.debug("Closed session with sessionId: {}", sessionId);
        }

        class SendConnectAckTask implements Callable<Boolean> {

            private final MessagePublisher publisher;

            private final MonoSink<?> sink;

            SendConnectAckTask(MonoSink<?> sink) {
                this.sink = sink;
                this.publisher = new MessagePublisher(logger, 0, 0);
            }

            @Override
            public Boolean call() {
                Exception cause = null;
                try {
                    long result = publisher.publish(clientControlPub, MessageType.CONNECT_ACK,
                            Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId), sessionId);
                    if (result > 0) {
                        logger.debug("Sent {} to {}", MessageType.CONNECT_ACK, AeronUtils.format(clientControlPub));
                        sink.success();
                        return true;
                    } else if (result != Publication.CLOSED) {
                        return false;
                    }
                } catch (Exception ex) {
                    cause = ex;
                }
                sink.error(new RuntimeException("Failed to send " + MessageType.CONNECT_ACK, cause));
                return true;
            }
        }

    }

}
