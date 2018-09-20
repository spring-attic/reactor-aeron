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

import java.nio.ByteBuffer;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Pooler;

final class AeronClientInbound implements AeronInbound, Disposable {

  private final ByteBufferFlux flux;

  private final io.aeron.Subscription serverDataSubscription;

  private final Pooler pooler;

  private final ClientDataMessageProcessor processor;

  AeronClientInbound(
      Pooler pooler, AeronWrapper wrapper, String channel, int streamId, long sessionId) {
    this.pooler = Objects.requireNonNull(pooler);
    this.serverDataSubscription =
        wrapper.addSubscription(channel, streamId, "to receive data from server on", sessionId);
    this.processor = new ClientDataMessageProcessor(sessionId);
    this.flux = new ByteBufferFlux(processor);

    pooler.addDataSubscription(serverDataSubscription, processor);
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    pooler.removeSubscription(serverDataSubscription);

    serverDataSubscription.close();
  }

  long getLastSignalTimeNs() {
    return processor.lastSignalTimeNs;
  }

  static class ClientDataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private final long sessionId;

    private volatile long lastSignalTimeNs = 0;

    private volatile Subscription subscription;

    private volatile Subscriber<? super ByteBuffer> subscriber;

    ClientDataMessageProcessor(long sessionId) {
      this.sessionId = sessionId;
    }

    @Override
    public void onSubscribe(org.reactivestreams.Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
      lastSignalTimeNs = System.nanoTime();
      if (sessionId != this.sessionId) {
        throw new RuntimeException(
            "Received " + MessageType.NEXT + " for unknown sessionId: " + sessionId);
      }
      subscriber.onNext(buffer);
    }

    @Override
    public void onComplete(long sessionId) {}

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      this.subscriber = subscriber;

      subscriber.onSubscribe(subscription);
    }
  }
}
