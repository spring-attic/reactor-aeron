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

import reactor.ipc.aeron.AeronOptions;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClientOptions extends AeronOptions {

    private static final AtomicInteger nextClientStreamId = new AtomicInteger(0);

    private String clientChannel = "aeron:udp?endpoint=localhost:12001";

    private int clientStreamId = nextClientStreamId.incrementAndGet();

    private Duration ackTimeoutSecs = Duration.ofSeconds(10);

    public String clientChannel() {
        return clientChannel;
    }

    public void clientChannel(String clientChannel) {
        this.clientChannel = Objects.requireNonNull(clientChannel, "clientChannel");
    }

    public int clientStreamId() {
        return clientStreamId;
    }

    public Duration ackTimeoutSecs() {
        return ackTimeoutSecs;
    }

    public void ackTimeoutSecs(Duration ackTimeoutSecs) {
        this.ackTimeoutSecs = Objects.requireNonNull(ackTimeoutSecs, "ackTimeoutSecs");
    }
}
