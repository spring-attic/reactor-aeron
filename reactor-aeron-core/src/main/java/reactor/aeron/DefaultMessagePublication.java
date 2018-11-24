package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class DefaultMessagePublication implements Disposable, MessagePublication {

  private static final Logger logger = Loggers.getLogger(DefaultMessagePublication.class);

  private final IdleStrategy idleStrategy;

  private final long waitConnectedMillis;

  private final long waitBackpressuredMillis;

  private final Publication publication;

  private final String category;

  private final int mtuLength;

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  /**
   * Constructor.
   *
   * @param publication publication
   * @param category category
   * @param waitConnectedMillis wait connect millis
   * @param waitBackpressuredMillis wait backpressure millis
   */
  public DefaultMessagePublication(
      AeronResources aeronResources,
      Publication publication,
      String category,
      long waitConnectedMillis,
      long waitBackpressuredMillis) {
    this.mtuLength = aeronResources.mtuLength();
    this.publication = publication;
    this.category = category;
    this.idleStrategy = AeronUtils.newBackoffIdleStrategy();
    this.waitConnectedMillis = waitConnectedMillis;
    this.waitBackpressuredMillis = waitBackpressuredMillis;
  }

  @Override
  public long publish(MessageType msgType, ByteBuffer msgBody, long sessionId) {
    int capacity = msgBody.remaining() + Protocol.HEADER_SIZE;
    if (capacity < mtuLength) {
      BufferClaim bufferClaim = bufferClaims.get();
      long result = execute(() -> publication.tryClaim(capacity, bufferClaim));
      if (result > 0) {
        try {
          MutableDirectBuffer buffer = bufferClaim.buffer();
          int index = bufferClaim.offset();
          index = Protocol.putHeader(buffer, index, msgType, sessionId);
          buffer.putBytes(index, msgBody, msgBody.position(), msgBody.limit());
          bufferClaim.commit();
        } catch (Exception ex) {
          bufferClaim.abort();
          throw new RuntimeException("Unexpected exception", ex);
        }
      }
      return result;
    } else {
      UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgBody.remaining() + Protocol.HEADER_SIZE]);
      int index = Protocol.putHeader(buffer, 0, msgType, sessionId);
      buffer.putBytes(index, msgBody, msgBody.remaining());
      return execute(() -> publication.offer(buffer));
    }
  }

  private long execute(Supplier<Long> sendingTask) {
    long start = System.currentTimeMillis();
    long result;
    for (; ; ) {
      result = sendingTask.get();

      if (result > 0) {
        break;
      } else if (result == Publication.NOT_CONNECTED) {
        if (System.currentTimeMillis() - start > waitConnectedMillis) {
          logger.debug(
              "[{}] Publication NOT_CONNECTED: {} during {} millis",
              category,
              asString(),
              waitConnectedMillis);
          break;
        }
      } else if (result == Publication.BACK_PRESSURED) {
        if (System.currentTimeMillis() - start > waitBackpressuredMillis) {
          logger.debug(
              "[{}] Publication BACK_PRESSURED during {} millis: {}",
              category,
              asString(),
              waitBackpressuredMillis);
          break;
        }
      } else if (result == Publication.CLOSED) {
        logger.debug("[{}] Publication CLOSED: {}", category, AeronUtils.format(publication));
        break;
      } else if (result == Publication.ADMIN_ACTION) {
        if (System.currentTimeMillis() - start > waitConnectedMillis) {
          logger.debug(
              "[{}] Publication ADMIN_ACTION: {} during {} millis",
              category,
              asString(),
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
