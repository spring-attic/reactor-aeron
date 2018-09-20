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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.HeartbeatWatchdog;
import reactor.ipc.aeron.MessageType;
import reactor.util.Logger;
import reactor.util.Loggers;

class ClientControlMessageSubscriber implements ControlMessageSubscriber {

  private final Logger logger = Loggers.getLogger(ClientControlMessageSubscriber.class);

  private final String category;

  private final HeartbeatWatchdog heartbeatWatchdog;

  private final Map<UUID, MonoProcessor<ConnectAckResponse>> sinkByConnectRequestId =
      new ConcurrentHashMap<>();

  ClientControlMessageSubscriber(String category, HeartbeatWatchdog heartbeatWatchdog) {
    this.category = category;
    this.heartbeatWatchdog = heartbeatWatchdog;
  }

  @Override
  public void onSubscribe(org.reactivestreams.Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.debug(
        "[{}] Received {} for connectRequestId: {}, serverSessionStreamId: {}",
        category,
        MessageType.CONNECT_ACK,
        connectRequestId,
        serverSessionStreamId);

    MonoProcessor<ConnectAckResponse> processor = sinkByConnectRequestId.remove(connectRequestId);
    if (processor != null) {
      processor.onNext(new ConnectAckResponse(sessionId, serverSessionStreamId));
      processor.onComplete();
    }
  }

  @Override
  public void onHeartbeat(long sessionId) {
    heartbeatWatchdog.heartbeatReceived(sessionId);
  }

  @Override
  public void onConnect(
      UUID connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {
    logger.error(
        "[{}] Unsupported {} request for a client, clientChannel: {}, clientControlStreamId: {}, clientSessionStreamId: {}",
        category,
        MessageType.CONNECT,
        clientChannel,
        clientControlStreamId,
        clientSessionStreamId);
  }

  ConnectAckSubscription subscribeForConnectAck(UUID connectRequestId) {
    MonoProcessor<ConnectAckResponse> processor = MonoProcessor.create();
    sinkByConnectRequestId.put(connectRequestId, processor);
    return new ConnectAckSubscription(processor, connectRequestId);
  }

  class ConnectAckSubscription implements Disposable {

    private final MonoProcessor<ConnectAckResponse> processor;

    private final UUID connectRequestId;

    ConnectAckSubscription(MonoProcessor<ConnectAckResponse> processor, UUID connectRequestId) {
      this.processor = processor;
      this.connectRequestId = connectRequestId;
    }

    Mono<ConnectAckResponse> connectAck() {
      return processor;
    }

    @Override
    public void dispose() {
      sinkByConnectRequestId.remove(connectRequestId);
      processor.cancel();
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
}
