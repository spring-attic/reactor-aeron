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

import reactor.core.Disposable;
import reactor.core.publisher.TopicProcessor;
import reactor.ipc.aeron.AeronFlux;
import reactor.ipc.aeron.AeronInbound;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
final class AeronServerInbound implements AeronInbound, Disposable {

    private final AeronFlux flux;

    private final TopicProcessor<ByteBuffer> processor;

    private volatile long lastSignalTimeNs;

    AeronServerInbound(String name) {
        this.processor = TopicProcessor.<ByteBuffer>builder().name(name).build();
        this.flux = new AeronFlux(processor);
    }

    @Override
    public AeronFlux receive() {
        return flux;
    }

    void onNext(ByteBuffer buffer) {
        //FIXME: Could affect performance
        lastSignalTimeNs = System.nanoTime();
        processor.onNext(buffer);
    }

    @Override
    public void dispose() {
        processor.onComplete();
    }

    long getLastSignalTimeNs() {
        return lastSignalTimeNs;
    }
}
