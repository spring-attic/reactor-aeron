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

import io.aeron.Aeron;

import java.time.Duration;

/**
 * @author Anatoly Kadyshev
 */
public class AeronOptions {

    private static final String DEFAULT_SERVER_CHANNEL = "aeron:udp?endpoint=localhost:12000";

    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

    private static final int DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

    private String serverChannel = DEFAULT_SERVER_CHANNEL;

    private int serverStreamId = 1;

    private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;

    private Aeron aeron;

    private int backpressureTimeoutMillis = DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS;

    public int connectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void connectTimeoutMillis(int connectTimeoutMillis) {
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis > 0 expected, but got: " + connectTimeoutMillis);
        }

        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void serverChannel(String serverChannel) {
        this.serverChannel = serverChannel;
    }

    public String serverChannel() {
        return serverChannel;
    }

    public int serverStreamId() {
        return serverStreamId;
    }

    public void serverStreamId(int serverStreamId) {
        if (serverStreamId <= 0) {
            throw new IllegalArgumentException("serverStreamId > 0 expected, but got: " + serverStreamId);
        }

        this.serverStreamId = serverStreamId;
    }

    public Aeron getAeron() {
        return aeron;
    }

    public void aeron(Aeron aeron) {
        this.aeron = aeron;
    }

    public int backpressureTimeoutMillis() {
        return backpressureTimeoutMillis;
    }

    public void backpressureTimeoutMillis(int backpressureTimeoutMillis) {
        if (backpressureTimeoutMillis <= 0) {
            throw new IllegalArgumentException("backpressureTimeoutMillis > 0 expected, but got: " + backpressureTimeoutMillis);
        }

        this.backpressureTimeoutMillis = backpressureTimeoutMillis;
    }
}
