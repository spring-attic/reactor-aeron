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
import java.time.Duration;
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

    private static int clientStreamIdCounter = 0;

    private volatile Pooler pooler;

    private final Logger logger;

    private final AeronWrapper wrapper;

    private final AckMessageHandler ackMessageHandler;

    private final AtomicInteger handlersCounter = new AtomicInteger(0);

    private Subscription ackSubscription;

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
        this.logger = Loggers.getLogger(AeronClient.class + "." + this.name);
        this.wrapper = new AeronWrapper(this.name, options);
        this.ackMessageHandler = new AckMessageHandler();
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
        if (handlersCounter.getAndIncrement() == 0) {
            initialiseAckSubscription();
        }
        ClientHandler handler = new ClientHandler(ioHandler);
        return handler.start();
    }

    private synchronized void initialiseAckSubscription() {
        if (pooler == null) {
            ackSubscription = wrapper.addSubscription(options.clientChannel(), options.clientStreamId(), "server acks", null);
            Pooler pooler = new Pooler(this.name);
            pooler.addSubscription(ackSubscription, ackMessageHandler);
            pooler.initialise();
            this.pooler = pooler;
        }
    }

    private synchronized void closeAckSubscription() {
        pooler.removeSubscription(ackSubscription);
        pooler.shutdown();
        ackSubscription.close();
        ackSubscription = null;
        pooler = null;
    }

    class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final UUID sessionId;

        private final AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final int streamId;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
            this.ioHandler = ioHandler;
            this.sessionId = UUIDUtils.create();

            this.outbound = new AeronOutbound(name, wrapper,
                    options.serverChannel(), sessionId, options);

            this.streamId = options.clientStreamId() + clientStreamIdCounter++;
            this.inbound = new AeronClientInbound(pooler, wrapper,
                    options.clientChannel(), streamId, sessionId, name);
        }

        Mono<Disposable> start() {
            return ackMessageHandler.awaitConnectAck(sessionId)
                    .and(sendConnect(options.connectTimeoutMillis()), (serverStreamId, avoid) -> serverStreamId)
                    .map(outbound::initialise)
                    .doOnSuccess(avoid ->
                            Mono.from(ioHandler.apply(inbound, outbound))
                                    .doOnTerminate((aVoid, th) -> dispose())
                                    .subscribe())
                    .then(Mono.just(this));
        }

        Mono<Void> sendConnect(int timeoutMillis) {
            //FIXME:
            Publication publication = wrapper.addPublication(options.serverChannel(), options.serverStreamId(),
                    "sending requests", sessionId);

            return Mono.create(sink -> {
                ByteBuffer buffer = Protocol.createConnectBody(options.clientChannel(), streamId, options.clientStreamId());
                long result = 0;
                Exception cause = null;
                if (logger.isDebugEnabled()) {
                    logger.debug("Connecting to server at: {}", AeronUtils.format(publication));
                }
                MessagePublisher publisher = new MessagePublisher(logger, timeoutMillis, timeoutMillis);
                try {
                    result = publisher.publish(publication, MessageType.CONNECT, buffer, sessionId);
                } catch (Exception ex) {
                    cause = ex;
                }
                if (result > 0) {
                    sink.success();
                } else {
                    String message = "Failed to connect to server";
                    sink.error(cause == null ? new Exception(message) : new Exception(message, cause));
                }
            });
        }

        @Override
        public void dispose() {
            inbound.dispose();
            wrapper.dispose();
            outbound.dispose();

            if (handlersCounter.decrementAndGet() == 0) {
                closeAckSubscription();
            }
        }
    }

    //FIXME: Rename into ControlMessageHandler?
    private class AckMessageHandler implements MessageHandler {

        private final Map<UUID, MonoSink<Integer>> sinkByUuid = new ConcurrentHashMap<>();

        @Override
        public void onConnectAck(UUID sessionId, int serverStreamId) {
            logger.debug("Received {} for sessionId: {}, serverStreamId: {}", MessageType.CONNECT_ACK,
                    sessionId, serverStreamId);

            MonoSink<Integer> sink = sinkByUuid.get(sessionId);
            if (sink == null) {
                logger.error("Could not find sessionId: {}", sessionId);
            } else {
                sink.success(serverStreamId);
            }
        }

        Mono<Integer> awaitConnectAck(UUID sessionId) {
            return Mono.<Integer>create(sink -> {
                sinkByUuid.put(sessionId, sink);
                sink.onDispose(() ->
                        sinkByUuid.remove(sessionId));
            }).timeout(Duration.ofSeconds(5));
        }

    }
}
