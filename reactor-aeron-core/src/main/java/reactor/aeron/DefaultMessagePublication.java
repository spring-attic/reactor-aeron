package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
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

  /**
   * Constructor.
   *
   * @param publication publication
   * @param category category
   * @param waitConnectedMillis wait connect millis
   * @param waitBackpressuredMillis wait backpressure millis
   */
  public DefaultMessagePublication(
      Publication publication,
      String category,
      long waitConnectedMillis,
      long waitBackpressuredMillis) {
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
    for (; ; ) {
      result = publication.tryClaim(size, bufferClaim);

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
    if (!isDisposed()) {
      CloseHelper.quietClose(publication);
    }
  }

  @Override
  public boolean isDisposed() {
    return publication.isClosed();
  }
}
