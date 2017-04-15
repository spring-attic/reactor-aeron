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

import reactor.util.Logger;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronUtils {

    public static final String LABEL_PREFIX_SENDER_POS = "sender pos";

    public static final String LABEL_PREFIX_SUBSCRIBER_POS = "subscriber pos";

    public static BackoffIdleStrategy newBackoffIdleStrategy() {
        return new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    }

    public static long publish(Logger logger,
                               Publication publication,
                               RequestType requestType,
                               ByteBuffer byteBuffer,
                               IdleStrategy idleStrategy,
                               UUID sessionId,
                               long waitConnectedMillis,
                               long waitBackpressuredMillis) {
        long start = System.currentTimeMillis();
        long result;
        BufferClaim bufferClaim = new BufferClaim();
        for ( ; ; ) {
            int headerSize = BitUtil.SIZE_OF_BYTE + BitUtil.SIZE_OF_LONG * 2;
            //FIXME: Should consider buffer.position()?
            result = publication.tryClaim(byteBuffer.limit() + headerSize, bufferClaim);
            if (result > 0) {
                try {
                    MutableDirectBuffer buffer = bufferClaim.buffer();
                    int index = bufferClaim.offset();
                    index = putHeader(buffer, index, requestType, sessionId);
                    buffer.putBytes(index, byteBuffer, 0, byteBuffer.limit());
                    bufferClaim.commit();
                } catch (Exception ex) {
                    bufferClaim.abort();
                    throw ex;
                }
                break;
            } else if (result == Publication.NOT_CONNECTED || result == Publication.ADMIN_ACTION) {
                if (System.currentTimeMillis() - start > waitConnectedMillis) {
                    logger.debug("Failed to publish signal into {}, type: {}, byteBuffer: {}",
                            format(publication), requestType, byteBuffer);
                    break;
                }
            } else if (result == Publication.BACK_PRESSURED) {
                if (System.currentTimeMillis() - start > waitBackpressuredMillis) {
                    logger.debug("Publication BACK_PRESSURED: {}", format(publication));
                    break;
                }
            } else if (result == Publication.CLOSED) {
                logger.debug("Publication CLOSED: {}", format(publication));
                break;
            }

            idleStrategy.idle(0);
        }
        idleStrategy.reset();
        return result;
    }

    public static String format(Publication publication) {
        return publication.channel() + "/" + publication.streamId();
    }

    public static int putHeader(MutableDirectBuffer buffer, int index, RequestType requestType, UUID sessionId) {
        buffer.putByte(index, (byte) requestType.ordinal());
        index += BitUtil.SIZE_OF_BYTE;
        buffer.putLong(index, sessionId.getMostSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        buffer.putLong(index, sessionId.getLeastSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        return index;
    }

    public static ByteBuffer createConnectBody(String clientChannel, int clientStreamId) {
        byte[] bytes = new byte[1024];
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        int index = 0;
        index += buffer.putStringUtf8(index, clientChannel);
        buffer.putInt(index, clientStreamId);
        index += BitUtil.SIZE_OF_INT;
        return ByteBuffer.wrap(bytes, 0, index);
    }

    public static String byteBufferToString(ByteBuffer buffer) {
        return new String(buffer.array(), buffer.position(), buffer.limit());
    }
}
