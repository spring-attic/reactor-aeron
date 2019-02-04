package reactor.aeron.demo.pure;

import io.aeron.Image;
import io.aeron.Subscription;

/** Configuration used for samples with defaults which can be overridden by system properties. */
public interface Configurations {
  int FRAGMENT_COUNT_LIMIT = Integer.getInteger("reactor.aeron.sample.frameCountLimit", 10);
  int MESSAGE_LENGTH = Integer.getInteger("reactor.aeron.sample.messageLength", 32);
  int WARMUP_NUMBER_OF_ITERATIONS = Integer.getInteger("reactor.aeron.sample.warmup.iterations", 5);
  long WARMUP_NUMBER_OF_MESSAGES = Long.getLong("reactor.aeron.sample.warmup.messages", 10_000);
  long NUMBER_OF_MESSAGES = Long.getLong("reactor.aeron.sample.messages", 10_000_000);
  boolean EXCLUSIVE_PUBLICATIONS =
      Boolean.getBoolean("reactor.aeron.sample.exclusive.publications");
  boolean EMBEDDED_MEDIA_DRIVER = Boolean.getBoolean("reactor.aeron.sample.embeddedMediaDriver");
  boolean INFO_FLAG = Boolean.getBoolean("reactor.aeron.sample.info");
  int PING_STREAM_ID = Integer.getInteger("reactor.aeron.sample.ping.streamId", 10);
  int PONG_STREAM_ID = Integer.getInteger("reactor.aeron.sample.pong.streamId", 10);
  String PING_CHANNEL =
      System.getProperty("reactor.aeron.sample.ping.channel", "aeron:udp?endpoint=localhost:40123");
  String PONG_CHANNEL =
      System.getProperty("reactor.aeron.sample.pong.channel", "aeron:udp?endpoint=localhost:40123");

  /**
   * Print the information for an available image to stdout.
   *
   * @param image that has been created
   */
  static void printAvailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Available image on %s streamId=%d sessionId=%d from %s",
            subscription.channel(),
            subscription.streamId(),
            image.sessionId(),
            image.sourceIdentity()));
  }

  /**
   * Print the information for an unavailable image to stdout.
   *
   * @param image that has gone inactive
   */
  static void printUnavailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Unavailable image on %s streamId=%d sessionId=%d",
            subscription.channel(), subscription.streamId(), image.sessionId()));
  }
}
