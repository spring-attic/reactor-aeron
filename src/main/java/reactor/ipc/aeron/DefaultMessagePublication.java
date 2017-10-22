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
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public final class DefaultMessagePublication implements Disposable, MessagePublication {

    private static final Logger logger = Loggers.getLogger(DefaultMessagePublication.class);

    private final IdleStrategy idleStrategy;

    private final long waitConnectedMillis;

    private final long waitBackpressuredMillis;

    private final Publication publication;

    private final String category;

    public DefaultMessagePublication(Publication publication, String category, long waitConnectedMillis, long waitBackpressuredMillis) {
        this.publication = publication;
        this.category = category;
        this.idleStrategy = AeronUtils.newBackoffIdleStrategy();
        this.waitConnectedMillis = waitConnectedMillis;
        this.waitBackpressuredMillis = waitBackpressuredMillis;
    }

    @Override
    public long publish(MessageType msgType, ByteBuffer msgBody, long sessionId) {
        BufferClaim bufferClaim = new BufferClaim();
        int headerSize = Protocol.HEADER_SIZE;
        int size = headerSize + msgBody.remaining();
        long result = tryClaim(bufferClaim, size);
        if (result > 0) {
            try {
                MutableDirectBuffer buffer = bufferClaim.buffer();
                int index = bufferClaim.offset();
                index = Protocol.putHeader(buffer, index, msgType, sessionId);
                buffer.putBytes(index, msgBody, 0, msgBody.limit());
                bufferClaim.commit();
            } catch (Exception ex) {
                bufferClaim.abort();
                throw new RuntimeException("Unexpected exception", ex);
            }
        }
        return result;
    }

    private long tryClaim(BufferClaim bufferClaim, int size) {
        long start = System.currentTimeMillis();
        long result;
        for ( ; ; ) {
            result = publication.tryClaim(size, bufferClaim);

            if (result > 0) {
                break;
            } else if (result == Publication.NOT_CONNECTED) {
                if (System.currentTimeMillis() - start > waitConnectedMillis) {
                    logger.debug("[{}] Publication NOT_CONNECTED: {} during {} millis", category, asString(),
                            waitConnectedMillis);
                    break;
                }
            } else if (result == Publication.BACK_PRESSURED) {
                if (System.currentTimeMillis() - start > waitBackpressuredMillis) {
                    logger.debug("[{}] Publication BACK_PRESSURED during {} millis: {}", category, asString(),
                            waitBackpressuredMillis);
                    break;
                }
            } else if (result == Publication.CLOSED) {
                logger.debug("[{}] Publication CLOSED: {}", category, AeronUtils.format(publication));
                break;
            } else if (result == Publication.ADMIN_ACTION) {
                if (System.currentTimeMillis() - start > waitConnectedMillis) {
                    logger.debug("[{}] Publication ADMIN_ACTION: {} during {} millis", category, asString(),
                            waitConnectedMillis);
                    break;
                }
            }

            idleStrategy.idle(0);
        }
        idleStrategy.reset();
        return result;
    }

    @Override
    public String asString() {
        return AeronUtils.format(publication);
    }

    @Override
    public void dispose() {
        publication.close();
    }

    @Override
    public boolean isDisposed() {
        return publication.isClosed();
    }

}
