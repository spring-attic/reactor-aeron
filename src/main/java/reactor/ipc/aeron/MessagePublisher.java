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
import io.aeron.logbuffer.BufferClaim;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import reactor.util.Logger;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public class MessagePublisher {

    private final Logger logger;

    private final IdleStrategy idleStrategy;

    private final long waitConnectedMillis;

    private final long waitBackpressuredMillis;

    public MessagePublisher(Logger logger, long waitConnectedMillis, long waitBackpressuredMillis) {
        this.logger = logger;
        this.idleStrategy = AeronUtils.newBackoffIdleStrategy();
        this.waitConnectedMillis = waitConnectedMillis;
        this.waitBackpressuredMillis = waitBackpressuredMillis;
    }

    public long publish(Publication publication, MessageType msgType, ByteBuffer msgBody, long sessionId) {
        BufferClaim bufferClaim = new BufferClaim();
        int headerSize = Protocol.HEADER_SIZE;
        int size = headerSize + msgBody.remaining();
        long result = tryClaim(bufferClaim, publication, size);
        if (result > 0) {
            try {
                MutableDirectBuffer buffer = bufferClaim.buffer();
                int index = bufferClaim.offset();
                index = Protocol.putHeader(buffer, index, msgType, sessionId);
                buffer.putBytes(index, msgBody, 0, msgBody.limit());
                bufferClaim.commit();
            } catch (Exception ex) {
                bufferClaim.abort();
                throw ex;
            }
        } else {
            logger.debug("Failed to publish message into {}, type: {}, body: {}",
                    AeronUtils.format(publication), msgType, msgBody);
        }
        return result;
    }

    private long tryClaim(BufferClaim bufferClaim, Publication publication, int size) {
        long start = System.currentTimeMillis();
        long result;
        for ( ; ; ) {
            result = publication.tryClaim(size, bufferClaim);

            if (result > 0) {
                break;
            } else if (result == Publication.NOT_CONNECTED) {
                if (System.currentTimeMillis() - start > waitConnectedMillis) {
                    logger.debug("Publication NOT_CONNECTED: {} during {} millis", AeronUtils.format(publication),
                            waitConnectedMillis);
                    break;
                }
            } else if (result == Publication.BACK_PRESSURED) {
                if (System.currentTimeMillis() - start > waitBackpressuredMillis) {
                    logger.debug("Publication BACK_PRESSURED during {} millis: {}", AeronUtils.format(publication),
                            waitBackpressuredMillis);
                    break;
                }
            } else if (result == Publication.CLOSED) {
                logger.debug("Publication CLOSED: {}", AeronUtils.format(publication));
                break;
            } else if (result == Publication.ADMIN_ACTION) {
                if (System.currentTimeMillis() - start > waitConnectedMillis) {
                    logger.debug("Publication ADMIN_ACTION: {} during {} millis", AeronUtils.format(publication),
                            waitConnectedMillis);
                    break;
                }
            }

            idleStrategy.idle(0);
        }
        idleStrategy.reset();
        return result;
    }

}
