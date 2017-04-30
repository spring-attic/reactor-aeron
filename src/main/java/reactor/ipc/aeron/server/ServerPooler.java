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

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.Pooler;
import reactor.ipc.aeron.PoolerFragmentHandler;
import reactor.ipc.aeron.SignalHandler;
import uk.co.real_logic.aeron.Subscription;

import java.util.Objects;

/**
 * @author Anatoly Kadyshev
 */
class ServerPooler {

    private final Pooler pooler;

    public ServerPooler(Subscription subscription, SignalHandler handler, String name) {
        Objects.requireNonNull(handler, "handler");

        this.pooler = new Pooler(subscription, new PoolerFragmentHandler(handler), name);
    }

    public void initialise() {
        pooler.initialise();
    }

    public Mono<Void> shutdown() {
        return pooler.shutdown();
    }

}
