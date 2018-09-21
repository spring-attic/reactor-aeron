package reactor.ipc.aeron;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;

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
    return '(' + minifyChannel(channel) + ", streamId: " + streamId + ')';
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    return new String(buffer.array(), buffer.position(), buffer.limit());
  }

  public static ByteBuffer stringToByteBuffer(String str) {
    return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
  }
}
