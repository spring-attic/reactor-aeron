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
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.MessageType;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClient implements AeronConnector {

    private final AeronClientOptions options;

    private final String name;

    private final AtomicInteger nextClientStreamId = new AtomicInteger(0);

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
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
        ClientHandler handler = new ClientHandler(ioHandler, name, options, nextClientStreamId.incrementAndGet());
        return handler.start();
    }

    static class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final AeronClientOptions options;

        private final String category;

        private final AeronWrapper wrapper;

        private final UUID sessionId;

        private final AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final Connector connector;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                      String name,
                      AeronClientOptions options,
                      int clientStreamId) {
            this.ioHandler = ioHandler;
            this.options = options;
            this.category = name;
            this.wrapper = new AeronWrapper(category, options);
            this.sessionId = UUIDUtils.create();

            this.outbound = new AeronOutbound(category, wrapper,
                    this.options.serverChannel(), this.options.serverStreamId(), sessionId, options);

            this.inbound = new AeronClientInbound(wrapper,
                    this.options.clientChannel(), clientStreamId, sessionId, name);

            this.connector = new Connector(category, wrapper,
                    this.options.serverChannel(), this.options.serverStreamId(),
                    this.options.clientChannel(), clientStreamId,
                    sessionId);
        }

        Mono<Disposable> start() {
            return connector.connect(options.connectTimeoutMillis())
                    .doOnSuccess(avoid ->
                            Mono.from(ioHandler.apply(inbound, outbound))
                                    .doOnTerminate((aVoid, th) -> dispose())
                                    .subscribe())
                    .then(Mono.just(this));
        }

        @Override
        public void dispose() {
            inbound.dispose();
            wrapper.dispose();
            outbound.dispose();
        }
    }

    static class Connector {

        private final Publication publication;

        private final Logger logger;

        private String clientChannel;

        private int clientStreamId;

        private final UUID sessionId;

        Connector(String category,
                  AeronWrapper wrapper,
                  String serverChannel, int serverStreamId,
                  String clientChannel, int clientStreamId, UUID sessionId) {
            this.clientChannel = clientChannel;
            this.clientStreamId = clientStreamId;
            this.sessionId = sessionId;
            this.publication = wrapper.addPublication(serverChannel, serverStreamId, "sending requests", sessionId);
            this.logger = Loggers.getLogger(AeronClient.class + "." + category);
        }

        Mono<Void> connect(int timeoutMillis) {
            return Mono.create(sink -> {
                ByteBuffer buffer = Protocol.createConnectBody(clientChannel, clientStreamId);
                long result = 0;
                Exception cause = null;
                logger.debug("Connecting to server at: {}", AeronUtils.format(publication));
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
                    sink.error(cause == null ? new Exception(message): new Exception(message, cause));
                }
            });
        }
    }

}
