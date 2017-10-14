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
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.DefaultAeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.MessagePublisher;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Protocol;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @author Anatoly Kadyshev
 */
final class ClientConnector implements Disposable {

    private static final Logger logger = Loggers.getLogger(ClientConnector.class);

    private final String category;

    private final AeronClientOptions options;

    private final UUID connectRequestId;

    private final ClientControlMessageSubscriber controlMessageSubscriber;

    private final int clientControlStreamId;

    private final int clientSessionStreamId;

    private final Publication serverControlPublication;

    private final HeartbeatSender heartbeatSender;

    private final DefaultAeronOutbound outbound;

    private volatile long sessionId;

    private volatile Disposable heartbeatSenderDisposable = () -> {};

    ClientConnector(String category,
                    AeronWrapper wrapper,
                    AeronClientOptions options,
                    ClientControlMessageSubscriber controlMessageSubscriber,
                    HeartbeatSender heartbeatSender,
                    DefaultAeronOutbound outbound,
                    int clientControlStreamId,
                    int clientSessionStreamId) {
        this.category = category;
        this.options = options;
        this.controlMessageSubscriber = controlMessageSubscriber;
        this.clientControlStreamId = clientControlStreamId;
        this.clientSessionStreamId = clientSessionStreamId;
        this.heartbeatSender = heartbeatSender;
        this.outbound = outbound;
        this.connectRequestId = UUIDUtils.create();
        this.serverControlPublication = wrapper.addPublication(options.serverChannel(), options.serverStreamId(),
                "to send control requests to server", 0);
    }

    Mono<ClientControlMessageSubscriber.ConnectAckResponse> connect() {
        ClientControlMessageSubscriber.ConnectAckSubscription connectAckSubscription =
                controlMessageSubscriber.subscribeForConnectAck(connectRequestId);

        return sendConnectRequest()
                .then(connectAckSubscription.connectAck()
                        .timeout(options.ackTimeout())
                        .onErrorMap(TimeoutException.class, th -> {
                            throw new RuntimeException(
                                    String.format("Failed to receive %s during %d millis",
                                            MessageType.CONNECT_ACK, options.ackTimeout().toMillis()), th);
                        })
                )
                .doOnSuccess(response -> {
                    this.sessionId = response.sessionId;

                    heartbeatSenderDisposable = heartbeatSender.scheduleHeartbeats(serverControlPublication, sessionId)
                            .subscribe(avoid -> {}, th -> {});

                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] Successfully connected to server at {}, sessionId: {}", category,
                                AeronUtils.format(serverControlPublication), sessionId);
                    }
                })
                .doOnTerminate(connectAckSubscription::dispose)
                .onErrorMap(th -> {
                    throw new RuntimeException(String.format("Failed to connect to server at %s",
                                AeronUtils.format(serverControlPublication)));
                });
    }

    private Mono<Void> sendConnectRequest() {
        ByteBuffer buffer = Protocol.createConnectBody(connectRequestId, options.clientChannel(),
                clientControlStreamId, clientSessionStreamId);
        return Mono.fromRunnable(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Connecting to server at {}", category, AeronUtils.format(serverControlPublication));
            }
        }).then(send(buffer, MessageType.CONNECT, serverControlPublication, options.connectTimeoutMillis()));
    }

    private Mono<Void> sendDisconnectRequest() {
        ByteBuffer buffer = Protocol.createDisconnectBody(sessionId);
        return Mono.fromRunnable(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Disconnecting from server at {}", category, AeronUtils.format(serverControlPublication));
            }
        }).then(send(buffer, MessageType.COMPLETE, outbound.getPublication(), options.connectTimeoutMillis()));
    }

    private Mono<Void> send(ByteBuffer buffer, MessageType messageType, Publication publication, long timeoutMillis) {
        return Mono.create(sink -> {
            MessagePublisher publisher = new MessagePublisher(category, timeoutMillis, timeoutMillis);
            Exception cause = null;
            try {
                long result = publisher.publish(publication, messageType, buffer, sessionId);
                if (result > 0) {
                    logger.debug("[{}] Sent {} to {}", category, messageType, AeronUtils.format(publication));
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
        sendDisconnectRequest().subscribe(avoid -> {}, th -> {});

        heartbeatSenderDisposable.dispose();

        serverControlPublication.close();
    }

}
