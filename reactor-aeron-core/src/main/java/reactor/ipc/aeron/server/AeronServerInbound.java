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

import io.aeron.Subscription;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.TopicProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;
import reactor.util.Logger;
import reactor.util.Loggers;

/** @author Anatoly Kadyshev */
final class AeronServerInbound implements AeronInbound, Disposable {

  private final ByteBufferFlux flux;

  private final TopicProcessor<ByteBuffer> processor;

  private final Pooler pooler;

  private final Subscription serverDataSubscription;

  private final ServerDataMessageProcessor messageProcessor;

  AeronServerInbound(
      String name,
      AeronWrapper wrapper,
      AeronOptions options,
      Pooler pooler,
      int serverSessionStreamId,
      long sessionId,
      Runnable onCompleteHandler) {
    this.processor = TopicProcessor.<ByteBuffer>builder().name(name).build();
    this.pooler = pooler;
    this.flux = new ByteBufferFlux(processor);

    this.serverDataSubscription =
        wrapper.addSubscription(
            options.serverChannel(), serverSessionStreamId, "to receive client data on", sessionId);

    this.messageProcessor = new ServerDataMessageProcessor(name, sessionId, onCompleteHandler);
  }

  void initialise() {
    pooler.addDataSubscription(serverDataSubscription, messageProcessor);

    messageProcessor.subscribe(processor);
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    processor.onComplete();

    pooler.removeSubscription(serverDataSubscription);
    serverDataSubscription.close();
  }

  long lastSignalTimeNs() {
    return messageProcessor.lastSignalTimeNs;
  }

  static class ServerDataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private static final Logger logger = Loggers.getLogger(ServerDataMessageProcessor.class);

    private final String category;

    private volatile org.reactivestreams.Subscription subscription;

    private volatile long lastSignalTimeNs;

    private volatile Subscriber<? super ByteBuffer> subscriber;

    private final long sessionId;

    private final Runnable onCompleteHandler;

    ServerDataMessageProcessor(String category, long sessionId, Runnable onCompleteHandler) {
      this.category = category;
      this.sessionId = sessionId;
      this.onCompleteHandler = onCompleteHandler;
    }

    @Override
    public void onSubscribe(org.reactivestreams.Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}, buffer: {}",
            category,
            MessageType.NEXT,
            sessionId,
            buffer);
      }

      lastSignalTimeNs = System.nanoTime();

      if (this.sessionId == sessionId) {
        subscriber.onNext(buffer);
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}", category, MessageType.NEXT, sessionId);
      }
    }

    @Override
    public void onComplete(long sessionId) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
      }

      if (this.sessionId == sessionId) {
        onCompleteHandler.run();
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}",
            category,
            MessageType.COMPLETE,
            sessionId);
      }
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(subscription);
    }
  }
}
