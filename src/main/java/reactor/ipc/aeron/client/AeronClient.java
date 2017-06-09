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
import reactor.ipc.aeron.MessageHandler;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.Protocol;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClient implements AeronConnector {

    private final AeronClientOptions options;

    private final String name;

    private static final AtomicInteger streamIdCounter = new AtomicInteger();

    //TODO: shutdown it
    private final Pooler pooler;

    private final Logger logger;

    private final AeronWrapper wrapper;

    private final ControlMessageHandler controlMessageHandler;

    private int handlersCounter = 0;

    private Subscription controlSubscription;

    private final int controlStreamId;

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
        this.logger = Loggers.getLogger(AeronClient.class + "." + this.name);
        this.wrapper = new AeronWrapper(this.name, options);
        this.controlMessageHandler = new ControlMessageHandler();
        this.pooler = new Pooler(this.name);
        this.controlStreamId = streamIdCounter.incrementAndGet();
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
        return handler.start();
    }

    synchronized void clientHandlerCreated() {
        if (handlersCounter++ == 0) {
            controlSubscription = wrapper.addSubscription(
                    options.clientChannel(), controlStreamId, "control requests", 0);
            pooler.addSubscription(controlSubscription, controlMessageHandler);
            pooler.initialise();
        }
    }

    synchronized void clientHandlerDestroyed() {
        if (--handlersCounter == 0) {
            pooler.removeSubscription(controlSubscription);
            controlSubscription.close();
            controlSubscription = null;
        }
    }

    class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final UUID connectRequestId;

        private final AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final int clientSessionStreamId;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
            this.ioHandler = ioHandler;
            this.connectRequestId = UUIDUtils.create();

            this.outbound = new AeronOutbound(name, wrapper, options.serverChannel(), options);

            this.clientSessionStreamId = streamIdCounter.incrementAndGet();
            this.inbound = new AeronClientInbound(pooler, wrapper,
                    options.clientChannel(), clientSessionStreamId);
        }

        Mono<Disposable> start() {
            Mono<ConnectAckData> connectAckMono = controlMessageHandler.awaitConnectAck(connectRequestId)
                    .timeout(options.ackTimeoutSecs());

            Mono<Void> connectMono = sendConnect(options.connectTimeoutMillis());
            return Mono.fromRunnable(() -> clientHandlerCreated())
                    .then(connectAckMono)
                    .untilOther(connectMono)
                    .flatMap(connectAckData -> {
                        inbound.initialise(connectAckData.sessionId);
                        return outbound.initialise(connectAckData.sessionId, connectAckData.serverSessionStreamId);
                    })
                    .doOnSuccess(avoid ->
                            Mono.from(ioHandler.apply(inbound, outbound))
                                    .doOnTerminate((aVoid, th) -> dispose())
                                    .subscribe()
                    )
                    .then(Mono.just(this));
        }

        Mono<Void> sendConnect(int timeoutMillis) {
            return Mono.create(sink -> {
                Publication serverControlPub = wrapper.addPublication(options.serverChannel(), options.serverStreamId(),
                        "sending requests", 0);

                ByteBuffer buffer = Protocol.createConnectBody(connectRequestId, options.clientChannel(),
                        controlStreamId, clientSessionStreamId);
                long result = 0;
                Exception cause = null;
                if (logger.isDebugEnabled()) {
                    logger.debug("Connecting to server at {}", AeronUtils.format(serverControlPub));
                }
                MessagePublisher publisher = new MessagePublisher(logger, timeoutMillis, timeoutMillis);
                try {
                    result = publisher.publish(serverControlPub, MessageType.CONNECT, buffer, 0);
                } catch (Exception ex) {
                    cause = ex;
                }
                if (result > 0) {
                    sink.success();
                } else {
                    String message = "Failed to connect to server";
                    sink.error(cause == null ? new Exception(message) : new Exception(message, cause));
                }

                serverControlPub.close();
            });
        }

        @Override
        public void dispose() {
            inbound.dispose();
            wrapper.dispose();
            outbound.dispose();

            clientHandlerDestroyed();
        }
    }

    static class ConnectAckData {

        final long sessionId;

        final int serverSessionStreamId;

        ConnectAckData(long sessionId, int serverSessionStreamId) {
            this.sessionId = sessionId;
            this.serverSessionStreamId = serverSessionStreamId;
        }

    }

    class ControlMessageHandler implements MessageHandler {

        private final Map<UUID, MonoSink<ConnectAckData>> sinkByConnectRequestId = new ConcurrentHashMap<>();

        @Override
        public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
            logger.debug("Received {} for connectRequestId: {}, serverSessionStreamId: {}", MessageType.CONNECT_ACK,
                    connectRequestId, serverSessionStreamId);

            MonoSink<ConnectAckData> sink = sinkByConnectRequestId.get(connectRequestId);
            if (sink == null) {
                logger.error("Could not find sessionId: {}", connectRequestId);
            } else {
                sink.success(new ConnectAckData(sessionId, serverSessionStreamId));
            }
        }

        Mono<ConnectAckData> awaitConnectAck(UUID connectRequestId) {
            return Mono.create(sink -> {
                sinkByConnectRequestId.put(connectRequestId, sink);
                sink.onDispose(() ->
                        sinkByConnectRequestId.remove(connectRequestId));
            });
        }

    }
}
