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

    //FIXME:
    public static final int CONNECT_ACK_TIMEOUT = 10;

    private final AeronClientOptions options;

    private final String name;

    private final AtomicInteger clientStreamIdCounter = new AtomicInteger();

    private volatile Pooler pooler;

    private final Logger logger;

    private final AeronWrapper wrapper;

    private final ControlMessageHandler controlMessageHandler;

    private final AtomicInteger handlersCounter = new AtomicInteger(0);

    private Subscription controlSubscription;

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
        this.logger = Loggers.getLogger(AeronClient.class + "." + this.name);
        this.wrapper = new AeronWrapper(this.name, options);
        this.controlMessageHandler = new ControlMessageHandler();
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
            initialiseControlSubscription();
        }
        ClientHandler handler = new ClientHandler(ioHandler);
        return handler.start();
    }

    private synchronized void initialiseControlSubscription() {
        if (pooler == null) {
            controlSubscription = wrapper.addSubscription(options.clientChannel(), options.clientStreamId(), "control requests", 0);
            Pooler pooler = new Pooler(this.name);
            pooler.addSubscription(controlSubscription, controlMessageHandler);
            pooler.initialise();
            this.pooler = pooler;
        }
    }

    private synchronized void closeControlSubscription() {
        pooler.removeSubscription(controlSubscription);
        pooler.shutdown();
        controlSubscription.close();
        controlSubscription = null;
        pooler = null;
    }

    class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final UUID connectRequestId;

        private final AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final int dataStreamId;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
            this.ioHandler = ioHandler;
            this.connectRequestId = UUIDUtils.create();

            this.outbound = new AeronOutbound(name, wrapper, options.serverChannel(), options);

            this.dataStreamId = options.clientStreamId() + clientStreamIdCounter.incrementAndGet();
            this.inbound = new AeronClientInbound(pooler, wrapper,
                    options.clientChannel(), dataStreamId, name);
        }


        Mono<Disposable> start() {
            Mono<ConnectAckData> connectAckMono = controlMessageHandler.awaitConnectAck(connectRequestId);
            Mono<Void> connectMono = sendConnect(options.connectTimeoutMillis());
            return connectAckMono.untilOther(connectMono)
                    .flatMap(connectAckData -> {
                        inbound.initialise(connectAckData.sessionId);
                        return outbound.initialise(connectAckData.sessionId, connectAckData.serverDataStreamId);
                    })
                    .doOnSuccess(avoid ->
                            Mono.from(ioHandler.apply(inbound, outbound))
                                    .doOnTerminate((aVoid, th) -> dispose())
                                    .subscribe()
                    )
                    .then(Mono.just(this));
        }

        Mono<Void> sendConnect(int timeoutMillis) {
            System.err.println("sendConnect called");

            //FIXME:
            Publication publication = wrapper.addPublication(options.serverChannel(), options.serverStreamId(),
                    "sending requests", 0);

            return Mono.create(sink -> {
                ByteBuffer buffer = Protocol.createConnectBody(connectRequestId, options.clientChannel(),
                        options.clientStreamId(), dataStreamId);
                long result = 0;
                Exception cause = null;
                if (logger.isDebugEnabled()) {
                    logger.debug("Connecting to server at: {}", AeronUtils.format(publication));
                }
                MessagePublisher publisher = new MessagePublisher(logger, timeoutMillis, timeoutMillis);
                try {
                    result = publisher.publish(publication, MessageType.CONNECT, buffer, 0);
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
                closeControlSubscription();
            }
        }
    }

    static class ConnectAckData {

        final long sessionId;

        final int serverDataStreamId;

        public ConnectAckData(long sessionId, int serverDataStreamId) {
            this.sessionId = sessionId;
            this.serverDataStreamId = serverDataStreamId;
        }

    }

    private class ControlMessageHandler implements MessageHandler {

        private final Map<UUID, MonoSink<ConnectAckData>> sinkByConnectRequestId = new ConcurrentHashMap<>();

        @Override
        public void onConnectAck(UUID connectRequestId, long sessionId, int serverDataStreamId) {
            logger.debug("Received {} for connectRequestId: {}, serverStreamId: {}", MessageType.CONNECT_ACK,
                    connectRequestId, serverDataStreamId);

            MonoSink<ConnectAckData> sink = sinkByConnectRequestId.get(connectRequestId);
            if (sink == null) {
                logger.error("Could not find sessionId: {}", connectRequestId);
            } else {
                sink.success(new ConnectAckData(sessionId, serverDataStreamId));
            }
        }

        Mono<ConnectAckData> awaitConnectAck(UUID connectRequestId) {
            System.err.println("awaitConnectAck called");
            return Mono.<ConnectAckData>create(sink -> {
                sinkByConnectRequestId.put(connectRequestId, sink);
                sink.onDispose(() ->
                        sinkByConnectRequestId.remove(connectRequestId));
            })
                    //FIXME: Move out?
                    .timeout(Duration.ofSeconds(CONNECT_ACK_TIMEOUT));
        }

    }
}
