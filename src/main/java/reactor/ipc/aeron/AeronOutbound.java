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
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.connector.Outbound;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronOutbound implements Outbound<ByteBuffer>, Disposable {

    private final WriteSequencer<ByteBuffer> sequencer;

    private final Scheduler scheduler;

    private final Publication publication;

    private final UUID sessionId;

    private final AeronOptions options;

    public AeronOutbound(String category,
                         AeronWrapper wrapper,
                         String channel,
                         int streamId,
                         UUID sessionId,
                         AeronOptions options) {
        this.sessionId = sessionId;
        this.options = options;
        this.publication = wrapper.addPublication(channel, streamId, "sending data", sessionId);
        Logger logger = Loggers.getLogger(AeronOutbound.class + "." + category);
        this.sequencer = new AeronWriteSequencer(logger, publication, options, sessionId);
        this.scheduler = Schedulers.newParallel("aeron-sender", 1);
    }

    @Override
    public Outbound<ByteBuffer> send(Publisher<? extends ByteBuffer> dataStream) {
        return then(sequencer.add(dataStream, scheduler));
    }

    @Override
    public void dispose() {
        scheduler.dispose();
    }

    public Mono<Void> initialise() {
        return Mono.create(sink -> {
            Scheduler scheduler = Schedulers.single();
            long startTime = System.currentTimeMillis();
            Runnable checkConnectedTask = new Runnable() {
                @Override
                public void run() {
                    if (System.currentTimeMillis() - startTime < options.connectTimeoutMillis()) {
                        if (!publication.isConnected()) {
                            scheduler.schedule(this, 500, TimeUnit.MILLISECONDS);
                        } else {
                            sink.success();
                        }
                    } else {
                        sink.error(new Exception("Failed to connect to client for sessionId: " + sessionId));
                    }
                }
            };
            scheduler.schedule(checkConnectedTask);
        });
    }

}
