package reactor.aeron;

import io.aeron.Publication;
import io.aeron.Subscription;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Aeron utils. */
public final class AeronUtils {

  public static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";

  /**
   * Returns formatted publication.
   *
   * @param publication publication
   * @return formatted publication
   */
  public static String format(Publication publication) {
    return format("p", publication.channel(), publication.streamId());
  }

  /**
   * Returns formatted subscription.
   *
   * @param subscription subscription
   * @return formatted subscription
   */
  public static String format(Subscription subscription) {
    return format("s", subscription.channel(), subscription.streamId());
  }

  /**
   * Returns formatted channel.
   *
   * @param type subscription or publciationo string type
   * @param channel channel
   * @param streamId stream id
   * @return formatter channel
   */
  public static String format(String type, String channel, int streamId) {
    return type + "|" + minifyChannel(channel) + "|streamId=" + streamId;
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
