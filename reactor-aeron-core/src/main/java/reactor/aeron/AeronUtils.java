package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Aeron utils. */
public final class AeronUtils {

  private static final String CHANNEL_PREFIX = "aeron:udp?endpoint=";

  private AeronUtils() {
    // Do not instantiate
  }

  /**
   * Returns formatted channel.
   *
   * @param category category of subscription or publication
   * @param type subscription or publication string type
   * @param channel channel string
   * @param streamId stream id
   * @return formatted channel
   */
  public static String format(String category, String type, String channel, int streamId) {
    return category + "|" + type + "|" + minifyChannel(channel) + "|streamId=" + streamId;
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
