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
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.SignalHandler;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * @author Anatoly Kadyshev
 */
final class AeronServerSignalHandler implements SignalHandler {

    private final Map<UUID, AeronServerInbound> inboundBySessionId;

    private final String category;

    private final AeronWrapper wrapper;

    private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

    private final AeronOptions options;

    private final Logger logger;

    public AeronServerSignalHandler(String category,
                                    AeronWrapper wrapper,
                                    BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                                    AeronOptions options) {
        this.category = category;
        this.wrapper = wrapper;
        this.ioHandler = ioHandler;
        this.options = options;
        this.inboundBySessionId = new HashMap<>();
        this.logger = Loggers.getLogger(AeronServer.class + "." + category);
    }

    @Override
    public void onConnect(UUID sessionId, String channel, int streamId) {
        logger.debug("Received CONNECT for sessionId: {}, channel/streamId: {}/{}", sessionId, channel, streamId);

        AeronOutbound outbound = new AeronOutbound(category, wrapper, channel, streamId, sessionId, options);
        AeronServerInbound inbound = new AeronServerInbound(category);
        inboundBySessionId.put(sessionId, inbound);

        outbound.initialise().doOnSuccess(avoid -> {
            Publisher<Void> publisher = ioHandler.apply(inbound, outbound);
            Mono.from(publisher).doOnTerminate((avoid2, th) -> {
                inboundBySessionId.remove(sessionId);
                outbound.dispose();
                inbound.dispose();

                logger.debug("Closed session with sessionId: {}", sessionId);
            }).subscribe();
        });
        //FIXME: Handler an error during AeronOutbound initialisation
    }

    @Override
    public void onNext(UUID sessionId, ByteBuffer buffer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Received NEXT for sessionId: {}, buffer: {}", sessionId, buffer);
        }

        AeronServerInbound inbound = inboundBySessionId.get(sessionId);
        if (inbound == null) {
            //FIXME: Handle
            logger.error("Could not find inbound for sessionId: {}", sessionId);
            return;
        }

        inbound.onNext(buffer);
    }
}
