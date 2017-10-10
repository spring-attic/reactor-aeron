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

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronServer implements AeronConnector {

    private final AeronOptions options;

    private final String name;

    public static AeronServer create(String name, Consumer<AeronOptions> optionsConfigurer) {
        return new AeronServer(name, optionsConfigurer);
    }

    public static AeronServer create(String name) {
        return create(name, options -> {});
    }

    public static AeronServer create() {
        return create(null);
    }

    private AeronServer(String name, Consumer<AeronOptions> optionsConfigurer) {
        this.name = name == null ? "server": name;
        AeronOptions options = new AeronOptions();
        optionsConfigurer.accept(options);
        this.options = options;
    }

    @Override
    public Mono<? extends Disposable> newHandler(
            BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
        Objects.requireNonNull(ioHandler, "ioHandler shouldn't be null");

        return Mono.create(sink -> {
            ServerHandler handler = new ServerHandler(name, ioHandler, options);
            handler.initialise();

            sink.success(handler);
        });
    }

}
