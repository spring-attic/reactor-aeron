package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

public final class MessagePublication implements OnDisposable, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  private final String category;
  private final Publication publication;
  private final AeronOptions options;
  private final AeronEventLoop eventLoop;

  private final ManyToOneConcurrentLinkedQueue<PublishTask> publishTasks =
      new ManyToOneConcurrentLinkedQueue<>();

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param category category
   * @param publication publication
   */
  MessagePublication(
      String category, Publication publication, AeronOptions options, AeronEventLoop eventLoop) {
    this.category = category;
    this.publication = publication;
    this.options = options;
    this.eventLoop = eventLoop;
  }

  /**
   * Enqueues buffer for future sending.
   *
   * @param buffer buffer
   * @return mono handle
   */
  public Mono<Void> enqueue(ByteBuffer buffer) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result = publishTasks.offer(new PublishTask(buffer, sink));
          }
          if (!result) {
            sink.error(AeronExceptions.failWithMessagePublicationUnavailable());
          }
        });
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return 1 - some progress was done; 0 - denotes no progress was done
   */
  public int proceed() {
    PublishTask task = publishTasks.peek();
    if (task == null) {
      return 0;
    }

    long result = task.run();
    if (result > 0) {
      publishTasks.poll();
      task.success();
      return 1;
    }

    // Handle closed publication
    if (result == Publication.CLOSED) {
      logger.warn("aeron.Publication is CLOSED: {}", this);
      dispose();
      return 0;
    }

    // Handle max position exceeded
    if (result == Publication.MAX_POSITION_EXCEEDED) {
      logger.warn("aeron.Publication received MAX_POSITION_EXCEEDED: {}", this);
      dispose();
      return 0;
    }

    Exception ex = null;

    // Handle failed connection
    if (result == Publication.NOT_CONNECTED) {
      if (task.isTimeoutElapsed(options.connectTimeout())) {
        logger.warn(
            "aeron.Publication failed to resolve NOT_CONNECTED within {} ms, {}",
            options.connectTimeout().toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve NOT_CONNECTED within timeout");
      }
    }

    // Handle backpressure
    if (result == Publication.BACK_PRESSURED) {
      if (task.isTimeoutElapsed(options.backpressureTimeout())) {
        logger.warn(
            "aeron.Publication failed to resolve BACK_PRESSURED within {} ms, {}",
            options.backpressureTimeout().toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve BACK_PRESSURED within timeout");
      }
    }

    // Handle admin action
    if (result == Publication.ADMIN_ACTION) {
      if (task.isTimeoutElapsed(options.connectTimeout())) {
        logger.warn(
            "aeron.Publication failed to resolve ADMIN_ACTION within {} ms, {}",
            options.connectTimeout().toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve ADMIN_ACTION within timeout");
      }
    }

    if (ex != null) {
      // Consume task and notify subscriber(s)
      publishTasks.poll();
      task.error(ex);
    }

    return 0;
  }

  @Override
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Can only close aeron publication from within event loop");
    }
    try {
      publication.close();
      logger.debug("aeron.Publication closed: {}", this);
    } finally {
      disposePublishTasks();
      onDispose.onComplete();
    }
  }

  @Override
  public void dispose() {
    eventLoop
        .dispose(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  @Override
  public boolean isDisposed() {
    return publication.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  public boolean isConnected() {
    return publication.isConnected();
  }

  private void disposePublishTasks() {
    for (; ; ) {
      PublishTask task = publishTasks.poll();
      if (task == null) {
        break;
      }
      task.error(AeronExceptions.failWithCancel("PublishTask has cancelled"));
    }
  }

  @Override
  public String toString() {
    return AeronUtils.format(category, "pub", publication.channel(), publication.streamId());
  }

  /**
   * Publish task.
   *
   * <p>Resident of {@link #publishTasks} queue.
   */
  private class PublishTask {

    private final ByteBuffer msgBody;
    private final MonoSink<Void> sink;

    private long start;

    private PublishTask(ByteBuffer msgBody, MonoSink<Void> sink) {
      this.msgBody = msgBody;
      this.sink = sink;
    }

    private long run() {
      if (start == 0) {
        start = System.currentTimeMillis();
      }

      int msgLength = msgBody.remaining();
      int position = msgBody.position();
      int limit = msgBody.limit();

      if (msgLength < publication.maxPayloadLength()) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = publication.tryClaim(msgLength, bufferClaim);
        if (result > 0) {
          try {
            MutableDirectBuffer dstBuffer = bufferClaim.buffer();
            int index = bufferClaim.offset();
            dstBuffer.putBytes(index, msgBody, position, limit);
            bufferClaim.commit();
          } catch (Exception ex) {
            bufferClaim.abort();
          }
        }
        return result;
      } else {
        return publication.offer(new UnsafeBuffer(msgBody, position, limit));
      }
    }

    private boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void success() {
      sink.success();
    }

    private void error(Throwable ex) {
      sink.error(ex);
    }
  }
}
