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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * @author Anatoly Kadyshev
 */
final class ServerHandler implements ControlMessageSubscriber, Disposable {

    private static final Logger logger = Loggers.getLogger(ServerHandler.class);

    private final String category;

    private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

    private final AeronWrapper wrapper;

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

    private final AeronOptions options;

    private final Pooler pooler;

    private final AtomicLong nextSessionId = new AtomicLong(0);

    private final HeartbeatWatchdog heartbeatWatchdog;

    private final Map<Long, SessionHandler> sessionHandlerById = new ConcurrentHashMap<>();

    private final HeartbeatSender heartbeatSender;

    private final io.aeron.Subscription controlSubscription;

    ServerHandler(String category,
                  BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                  AeronOptions options) {
        this.wrapper = new AeronWrapper(category, options);
        this.controlSubscription = wrapper.addSubscription(options.serverChannel(), options.serverStreamId(),
                "to receive control requests on", 0);

        this.category = category;
        this.ioHandler = ioHandler;
        this.options = options;
        this.pooler = new Pooler(category);
        this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), category);
        this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), category);
    }

    void initialise() {
        pooler.addControlSubscription(controlSubscription, this);
        pooler.initialise();
    }

    @Override
    public void dispose() {
        pooler.shutdown().doOnTerminate(() -> {
                    sessionHandlerById.values().forEach(SessionHandler::dispose);

                    controlSubscription.close();
                    wrapper.dispose();
                }).subscribe();
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onConnect(UUID connectRequestId, String clientChannel, int clientControlStreamId, int clientSessionStreamId) {
        logger.debug("[{}] Received {} for connectRequestId: {}, channel={}, clientControlStreamId={}, clientSessionStreamId={}",
                category, MessageType.CONNECT, connectRequestId, AeronUtils.minifyChannel(clientChannel),
                clientControlStreamId, clientSessionStreamId);

        int serverSessionStreamId = streamIdCounter.incrementAndGet();
        long sessionId = nextSessionId.incrementAndGet();
        SessionHandler sessionHandler = new SessionHandler(clientChannel, clientSessionStreamId, clientControlStreamId,
                connectRequestId, sessionId, serverSessionStreamId);

        sessionHandler.initialise()
                .subscribeOn(Schedulers.single())
                .subscribe();
    }

    @Override
    public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
        logger.error("[{}] Received unsupported server request {}, connectRequestId: {}", category,
                MessageType.CONNECT_ACK, connectRequestId);
    }

    @Override
    public void onHeartbeat(long sessionId) {
        heartbeatWatchdog.heartbeatReceived(sessionId);
    }

    class SessionHandler implements Disposable {

        private final Logger logger = Loggers.getLogger(SessionHandler.class);

        private final DefaultAeronOutbound outbound;

        private final AeronServerInbound inbound;

        private final int clientSessionStreamId;

        private final UUID connectRequestId;

        private final long sessionId;

        private final ServerConnector connector;

        SessionHandler(String clientChannel, int clientSessionStreamId, int clientControlStreamId,
                       UUID connectRequestId, long sessionId, int serverSessionStreamId) {
            this.clientSessionStreamId = clientSessionStreamId;
            this.outbound = new DefaultAeronOutbound(category, wrapper, clientChannel, options);
            this.connectRequestId = connectRequestId;
            this.sessionId = sessionId;
            this.inbound = new AeronServerInbound(category, wrapper, options, pooler, serverSessionStreamId, sessionId,
                    () -> dispose());
            this.connector = new ServerConnector(category, wrapper, clientChannel, clientControlStreamId,
                    sessionId, serverSessionStreamId, connectRequestId, options, heartbeatSender);
        }

        Mono<Void> initialise() {
            Mono<Void> initialiseOutbound = outbound.initialise(sessionId, clientSessionStreamId);

            return connector.connect()
                    .then(initialiseOutbound)
                    .doOnSuccess(avoid -> {
                        inbound.initialise();

                        heartbeatWatchdog.add(sessionId, () -> {
                                    heartbeatWatchdog.remove(sessionId);
                                    dispose();
                                },
                                () -> inbound.lastSignalTimeNs());

                        sessionHandlerById.put(sessionId, this);

                        logger.debug("[{}] Client with connectRequestId: {} successfully connected, sessionId: {}", category,
                                connectRequestId, sessionId);

                        Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
                        Mono.from(publisher).doOnTerminate(this::dispose).subscribe();
                    })
                    .doOnError(th -> {
                        logger.debug("[{}] Failed to connect to the client for sessionId: {}", category, sessionId, th);

                        dispose();
                    });
        }

        @Override
        public void dispose() {
            sessionHandlerById.remove(this);

            heartbeatWatchdog.remove(sessionId);

            connector.dispose();

            outbound.dispose();
            inbound.dispose();

            logger.debug("[{}] Closed session with sessionId: {}", category, sessionId);
        }

    }

}
