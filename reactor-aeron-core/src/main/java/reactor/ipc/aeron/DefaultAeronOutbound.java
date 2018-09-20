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
package reactor.ipc.aeron;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** @author Anatoly Kadyshev */
public final class DefaultAeronOutbound implements Disposable, AeronOutbound {

  private final Scheduler scheduler;

  private final String category;

  private final AeronWrapper wrapper;

  private final String channel;

  private final AeronOptions options;

  private volatile WriteSequencer<ByteBuffer> sequencer;

  private volatile DefaultMessagePublication publication;

  public DefaultAeronOutbound(
      String category, AeronWrapper wrapper, String channel, AeronOptions options) {
    this.category = category;
    this.wrapper = wrapper;
    this.channel = channel;
    this.options = options;
    this.scheduler = Schedulers.newSingle(category + "-[sender]", false);
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(sequencer.add(dataStream));
  }

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    scheduler.dispose();

    if (publication != null) {
      publication.dispose();
    }
  }

  public Mono<Void> initialise(long sessionId, int streamId) {
    return Mono.create(
        sink -> {
          Publication aeronPublication =
              wrapper.addPublication(channel, streamId, "to send data to", sessionId);
          this.publication =
              new DefaultMessagePublication(
                  aeronPublication,
                  category,
                  options.connectTimeoutMillis(),
                  options.backpressureTimeoutMillis());
          this.sequencer = new AeronWriteSequencer(scheduler, category, publication, sessionId);
          int timeoutMillis = options.connectTimeoutMillis();
          new RetryTask(
                  Schedulers.single(),
                  100,
                  timeoutMillis,
                  () -> {
                    if (aeronPublication.isConnected()) {
                      sink.success();
                      return true;
                    }
                    return false;
                  },
                  th ->
                      sink.error(
                          new Exception(
                              String.format(
                                  "Publication %s for sending data in not connected during %d millis",
                                  publication.asString(), timeoutMillis),
                              th)))
              .schedule();
        });
  }

  public MessagePublication getPublication() {
    return publication;
  }
}
