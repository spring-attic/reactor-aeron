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
package reactor.ipc.aeron.client;

import io.aeron.Publication;
import io.aeron.Subscription;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
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
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClient implements AeronConnector, Disposable {

    private final AeronClientOptions options;

    private final String name;

    private static final AtomicInteger streamIdCounter = new AtomicInteger();

    private final Pooler pooler;

    private final Logger logger;

    private final AeronWrapper wrapper;

    private final ControlMessageHandler controlMessageHandler;

    private final Subscription controlSubscription;

    private final int controlStreamId;

    private final HeartbeatSender heartbeatSender;

    private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

    private final HeartbeatWatchdog heartbeatWatchdog;

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
        this.logger = Loggers.getLogger(AeronClient.class + "." + this.name);
        this.controlMessageHandler = new ControlMessageHandler();
        this.controlStreamId = streamIdCounter.incrementAndGet();
        this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), this.name);
        this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), this.name);

        this.wrapper = new AeronWrapper(this.name, options);
        this.controlSubscription = wrapper.addSubscription(
                options.clientChannel(), controlStreamId, "to receive control requests on", 0);

        Pooler pooler = new Pooler(this.name);
        pooler.addSubscription(controlSubscription, controlMessageHandler);
        pooler.initialise();

        this.pooler = pooler;
    }

    public static AeronClient create(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        return new AeronClient(name, optionsConfigurer);
    }

    public static AeronClient create(String name) {
        return create(name, options -> {});
    }

    public static AeronClient create() {
        return create(null);
    }

    @Override
    public Mono<? extends Disposable> newHandler(
            BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
        ClientHandler handler = new ClientHandler(ioHandler);
        return handler.initialise();
    }

    @Override
    public void dispose() {
        handlers.forEach(ClientHandler::dispose);

        pooler.removeSubscription(controlSubscription);
        pooler.shutdown().block();

        controlSubscription.close();

        wrapper.dispose();
    }

    class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final UUID connectRequestId;

        private volatile AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final int clientSessionStreamId;

        private final Publication serverControlPub;

        private volatile Disposable heartbeatDisposable = () -> {};

        private volatile long sessionId;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
            this.ioHandler = ioHandler;
            this.connectRequestId = UUIDUtils.create();
            this.clientSessionStreamId = streamIdCounter.incrementAndGet();
            this.serverControlPub = wrapper.addPublication(options.serverChannel(), options.serverStreamId(),
                    "to send control requests to server", 0);
            this.outbound = new AeronOutbound(name, wrapper, options.serverChannel(), options);
        }

        Mono<Disposable> initialise() {
            handlers.add(this);

            Mono<ConnectAckResponse> awaitConnectAckMono = controlMessageHandler.awaitConnectAck(connectRequestId)
                    .timeout(options.ackTimeout());

            return awaitConnectAckMono.delayUntil(c -> sendConnectRequest())
                    .flatMap(connectAckResponse -> {
                        inbound = new AeronClientInbound(pooler, wrapper, options.clientChannel(),
                                clientSessionStreamId, connectAckResponse.sessionId);

                        heartbeatDisposable = heartbeatSender.scheduleHeartbeats(serverControlPub, connectAckResponse.sessionId)
                                .doOnError(th -> heartbeatDisposable.dispose())
                                .subscribe();

                        this.sessionId = connectAckResponse.sessionId;
                        heartbeatWatchdog.add(connectAckResponse.sessionId, this::dispose, ignore -> inbound.getLastSignalTimeNs());

                        return outbound.initialise(connectAckResponse.sessionId, connectAckResponse.serverSessionStreamId);
                    })
                    .doOnSuccess(ignore -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully connected to server at {}, sessionId: {}",
                                    AeronUtils.format(serverControlPub), sessionId);
                        }
                    })
                    .doOnSuccess(avoid -> Mono.from(ioHandler.apply(inbound, outbound))
                            .doOnTerminate((avoid2, th) -> dispose())
                            .subscribe())
                    .doOnError(th -> {
                        logger.error("Unexpected exception", th);
                        dispose();
                    })
                    .then(Mono.just(this));
        }

        Mono<Void> sendConnectRequest() {
            ByteBuffer buffer = Protocol.createConnectBody(connectRequestId, options.clientChannel(),
                    controlStreamId, clientSessionStreamId);
            return Mono.fromRunnable(() -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connecting to server at {}", AeronUtils.format(serverControlPub));
                }
            }).then(send(buffer, MessageType.CONNECT, serverControlPub, options.connectTimeoutMillis()));
        }

        Mono<Void> sendDisconnectRequest() {
            ByteBuffer buffer = Protocol.createDisconnectBody(sessionId);
            return Mono.fromRunnable(() -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Disconnecting from server at {}", AeronUtils.format(serverControlPub));
                }
            }).then(send(buffer, MessageType.COMPLETE, outbound.getPublication(), options.backpressureTimeoutMillis()));
        }

        //FIXME: Make static
        private Mono<Void> send(ByteBuffer buffer, MessageType messageType, Publication publication, long timeoutMillis) {
            return Mono.create(sink -> {
                MessagePublisher publisher = new MessagePublisher(logger, timeoutMillis, timeoutMillis);
                Exception cause = null;
                try {
                    long result = publisher.publish(publication, messageType, buffer, sessionId);
                    if (result > 0) {
                        logger.debug("Sent {} to {}", messageType, AeronUtils.format(publication));
                        sink.success();
                        return;
                    }
                } catch (Exception ex) {
                    cause = ex;
                }
                sink.error(new RuntimeException("Failed to send message of type: " + messageType, cause));
            });
        }

        @Override
        public void dispose() {
            sendDisconnectRequest().subscribe(aVoid -> {}, th -> {});

            handlers.remove(this);

            heartbeatDisposable.dispose();

            heartbeatWatchdog.remove(sessionId);

            if (inbound != null) {
                inbound.dispose();
            }
            outbound.dispose();

            serverControlPub.close();

            logger.debug("Closed session with Id: {}", sessionId);
        }
    }

    static class ConnectAckResponse {

        final long sessionId;

        final int serverSessionStreamId;

        ConnectAckResponse(long sessionId, int serverSessionStreamId) {
            this.sessionId = sessionId;
            this.serverSessionStreamId = serverSessionStreamId;
        }

    }

    class ControlMessageHandler implements MessageHandler {

        private final Map<UUID, MonoSink<ConnectAckResponse>> sinkByConnectRequestId = new ConcurrentHashMap<>();

        @Override
        public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
            logger.debug("Received {} for connectRequestId: {}, serverSessionStreamId: {}", MessageType.CONNECT_ACK,
                    connectRequestId, serverSessionStreamId);

            MonoSink<ConnectAckResponse> sink = sinkByConnectRequestId.get(connectRequestId);
            if (sink == null) {
                logger.error("Could not find connectRequestId: {}", connectRequestId);
            } else {
                sink.success(new ConnectAckResponse(sessionId, serverSessionStreamId));
            }
        }

        @Override
        public void onHeartbeat(long sessionId) {
            heartbeatWatchdog.heartbeatReceived(sessionId);
        }

        Mono<ConnectAckResponse> awaitConnectAck(UUID connectRequestId) {
            return Mono.create(sink -> {
                sinkByConnectRequestId.put(connectRequestId, sink);
                sink.onDispose(() ->
                        sinkByConnectRequestId.remove(connectRequestId));
            });
        }

    }
}
