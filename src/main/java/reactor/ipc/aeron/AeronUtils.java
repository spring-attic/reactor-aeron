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
import org.agrona.concurrent.BackoffIdleStrategy;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronUtils {

    public static final String LABEL_PREFIX_SENDER_POS = "sender pos";

    public static final String LABEL_PREFIX_SUBSCRIBER_POS = "subscriber pos";

    public static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";

    public static BackoffIdleStrategy newBackoffIdleStrategy() {
        return new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    }

    public static String format(Publication publication) {
        return format(publication.channel(), publication.streamId());
    }

    public static String minifyChannel(String channel) {
        if (channel.startsWith(CHANNEL_PREFIX)) {
            channel = channel.substring(CHANNEL_PREFIX.length());
        }
        return channel;
    }

    public static String format(String channel, int streamId) {
        return minifyChannel(channel) + ", streamId: " + streamId;
    }

    public static String byteBufferToString(ByteBuffer buffer) {
        return new String(buffer.array(), buffer.position(), buffer.limit());
    }
}
