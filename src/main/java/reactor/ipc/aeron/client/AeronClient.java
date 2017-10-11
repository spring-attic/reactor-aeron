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

import io.aeron.Subscription;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.Pooler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClient implements AeronConnector, Disposable {

    private static final Logger logger = Loggers.getLogger(AeronClient.class);

    private final AeronClientOptions options;

    private final String name;

    private static final AtomicInteger streamIdCounter = new AtomicInteger();

    private final Pooler pooler;

    private final AeronWrapper wrapper;

    private final ClientControlMessageSubscriber controlMessageSubscriber;

    private final Subscription controlSubscription;

    private final int clientControlStreamId;

    private final HeartbeatSender heartbeatSender;

    private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

    private final HeartbeatWatchdog heartbeatWatchdog;

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
        this.heartbeatWatchdog = new HeartbeatWatchdog(options.heartbeatTimeoutMillis(), this.name);
        this.controlMessageSubscriber = new ClientControlMessageSubscriber(name, heartbeatWatchdog);
        this.clientControlStreamId = streamIdCounter.incrementAndGet();
        this.heartbeatSender = new HeartbeatSender(options.heartbeatTimeoutMillis(), this.name);

        this.wrapper = new AeronWrapper(this.name, options);
        this.controlSubscription = wrapper.addSubscription(
                options.clientChannel(), clientControlStreamId, "to receive control requests on", 0);

        Pooler pooler = new Pooler(this.name);
        pooler.addControlSubscription(controlSubscription, controlMessageSubscriber);
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

        private final AeronOutbound outbound;

        private final int clientSessionStreamId;

        private final ClientConnector connector;

        private volatile AeronClientInbound inbound;

        private volatile long sessionId;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
            this.ioHandler = ioHandler;
            this.clientSessionStreamId = streamIdCounter.incrementAndGet();
            this.outbound = new AeronOutbound(name, wrapper, options.serverChannel(), options);
            this.connector = new ClientConnector(name, wrapper, options, controlMessageSubscriber,
                    heartbeatSender, outbound, clientControlStreamId, clientSessionStreamId);
        }

        Mono<Disposable> initialise() {
            handlers.add(this);

            return connector.connect()
                    .flatMap(connectAckResponse -> {
                        inbound = new AeronClientInbound(pooler, wrapper, options.clientChannel(),
                                clientSessionStreamId, connectAckResponse.sessionId);

                        this.sessionId = connectAckResponse.sessionId;
                        heartbeatWatchdog.add(connectAckResponse.sessionId, this::dispose, () -> inbound.getLastSignalTimeNs());

                        return outbound.initialise(connectAckResponse.sessionId, connectAckResponse.serverSessionStreamId);
                    })
                    .doOnSuccess(avoid ->
                            Mono.from(ioHandler.apply(inbound, outbound))
                                    .doOnTerminate(this::dispose)
                                    .subscribe()
                    )
                    .doOnError(th -> dispose())
                    .then(Mono.just(this));
        }

        @Override
        public void dispose() {
            connector.dispose();

            handlers.remove(this);

            heartbeatWatchdog.remove(sessionId);

            if (inbound != null) {
                inbound.dispose();
            }
            outbound.dispose();

            if (sessionId > 0) {
                logger.debug("[{}] Closed session with Id: {}", name, sessionId);
            }
        }
    }

}
