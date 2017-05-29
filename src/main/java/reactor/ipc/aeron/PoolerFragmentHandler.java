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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public class PoolerFragmentHandler implements FragmentHandler {

    private Logger logger = Loggers.getLogger(PoolerFragmentHandler.class);

    private final MessageHandler handler;

    public PoolerFragmentHandler(MessageHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        int index = offset;
        int type = buffer.getByte(index);
        index += BitUtil.SIZE_OF_BYTE;
        long sessionIdMostSigBits = buffer.getLong(index);
        index += BitUtil.SIZE_OF_LONG;
        long sessionIdLeastSigBits = buffer.getLong(index);
        index += BitUtil.SIZE_OF_LONG;
        UUID sessionId = new UUID(sessionIdMostSigBits, sessionIdLeastSigBits);

        if (type == MessageType.CONNECT.ordinal()) {
            int channelLength = buffer.getInt(index);
            String channel = buffer.getStringUtf8(index, channelLength);
            index += BitUtil.SIZE_OF_INT + channelLength;
            int streamId = buffer.getInt(index);
            index += BitUtil.SIZE_OF_INT;
            int clientAckStreamId = buffer.getInt(index);

            handler.onConnect(sessionId, channel, streamId, clientAckStreamId);
        } else if (type == MessageType.NEXT.ordinal()) {
            int bytesLength = length - (index - offset);
            ByteBuffer dst = ByteBuffer.allocate(bytesLength);
            buffer.getBytes(index, dst, bytesLength);
            dst.rewind();

            handler.onNext(sessionId, dst);
        } else if (type == MessageType.CONNECT_ACK.ordinal()) {
            int serverStreamId = buffer.getInt(index);

            handler.onConnectAck(sessionId, serverStreamId);
        } else {
            logger.error("Unknown message type id: {}", type);
        }
    }

}
