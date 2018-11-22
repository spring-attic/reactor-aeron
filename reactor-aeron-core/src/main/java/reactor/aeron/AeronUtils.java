package reactor.aeron;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;

/** Aeron utils. */
public final class AeronUtils {

  public static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";

  /**
   * Factory for backoff idle strategy.
   *
   * @return backoff idle strategy
   */
  public static BackoffIdleStrategy newBackoffIdleStrategy() {
    return new BackoffIdleStrategy(
        100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
  }

  /**
   * Returns formatted publication.
   *
   * @param publication publication
   * @return formatted publication
   */
  public static String format(Publication publication) {
    return format(publication.channel(), publication.streamId());
  }

  /**
   * Returns formatted channel.
   *
   * @param channel channel
   * @param streamId stream id
   * @return formatter channel
   */
  public static String format(String channel, int streamId) {
    return '(' + minifyChannel(channel) + ", streamId: " + streamId + ')';
  }

  /**
   * Returns channel w/o channel prefix.
   *
   * @param channel channel
   * @return channel w/o channel prefix
   */
  public static String minifyChannel(String channel) {
    if (channel.startsWith(CHANNEL_PREFIX)) {
      channel = channel.substring(CHANNEL_PREFIX.length());
    }
    return channel;
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    return new String(buffer.array(), buffer.position(), buffer.limit());
  }

  public static ByteBuffer stringToByteBuffer(String str) {
    return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
  }
}
