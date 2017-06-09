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

import reactor.core.publisher.FluxSink;
import reactor.ipc.aeron.MessageHandler;
import reactor.ipc.aeron.MessageType;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
final class ClientMessageHandler implements MessageHandler {

    private final long sessionId;

    private final FluxSink<ByteBuffer> emitter;

    public ClientMessageHandler(long sessionId, FluxSink<ByteBuffer> emitter) {
        Objects.requireNonNull(sessionId, "sessionId");

        this.sessionId = sessionId;
        this.emitter = emitter;
    }

    @Override
    public void onConnect(UUID connectRequestId, String clientChannel, int clientControlStreamId, int clientSessionStreamId) {
        throw new UnsupportedOperationException("Client doesn't support " + MessageType.CONNECT + " requests");
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
        if (sessionId != this.sessionId) {
            throw new RuntimeException("Received " + MessageType.NEXT + " for unknown sessionId: " + sessionId);
        }
        emitter.next(buffer);
    }

}
