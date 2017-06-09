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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronOutbound implements Outbound<ByteBuffer>, Disposable {

    private final Scheduler scheduler;

    private final String category;

    private final AeronWrapper wrapper;

    private final String channel;

    private final AeronOptions options;

    private volatile WriteSequencer<ByteBuffer> sequencer;

    private volatile Publication publication;

    public AeronOutbound(String category,
                         AeronWrapper wrapper,
                         String channel,
                         AeronOptions options) {
        this.category = category;
        this.wrapper = wrapper;
        this.channel = channel;
        this.options = options;
        this.scheduler = Schedulers.newParallel(category + "-[sender]", 1);
    }

    @Override
    public Outbound<ByteBuffer> send(Publisher<? extends ByteBuffer> dataStream) {
        return then(sequencer.add(dataStream, scheduler));
    }

    @Override
    public void dispose() {
        scheduler.dispose();
        publication.close();
    }

    public Mono<Void> initialise(long sessionId, int streamId) {
        this.publication = wrapper.addPublication(channel, streamId, "sending data", sessionId);
        this.sequencer = new AeronWriteSequencer(category, publication, options, sessionId);

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
