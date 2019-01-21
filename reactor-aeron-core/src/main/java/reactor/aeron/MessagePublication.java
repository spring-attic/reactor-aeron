package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.time.Duration;
import java.util.Queue;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

class MessagePublication implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  private final Publication publication;
  private final AeronEventLoop eventLoop;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration adminActionTimeout;

  private final Queue<PublishTask> publishTasks = new ManyToOneConcurrentLinkedQueue<>();

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param publication aeron publication
   * @param options aeron options
   * @param eventLoop aeron event loop where this {@code MessagePublication} is assigned
   */
  MessagePublication(Publication publication, AeronOptions options, AeronEventLoop eventLoop) {
    this.publication = publication;
    this.eventLoop = eventLoop;
    this.connectTimeout = options.connectTimeout();
    this.backpressureTimeout = options.backpressureTimeout();
    this.adminActionTimeout = options.adminActionTimeout();
  }

  /**
   * Enqueues content for future sending.
   *
   * @param buffer abstract buffer to send
   * @param bufferHandler abstract buffer handler
   * @return mono handle
   */
  <B> Mono<Void> publish(B buffer, DirectBufferHandler<? super B> bufferHandler) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result = publishTasks.offer(new PublishTask<>(buffer, bufferHandler, sink));
          }
          if (!result) {
            sink.error(AeronExceptions.failWithPublicationUnavailable());
          }
        });
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return 1 - some progress was done; 0 - denotes no progress was done
   */
  int proceed() {
    Exception ex = null;

    PublishTask task = publishTasks.peek();
    if (task == null) {
      return 0;
    }

    long result = 0;

    try {
      result = task.publish();
    } catch (Exception e) {
      ex = e;
    }

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

    // Handle failed connection
    if (result == Publication.NOT_CONNECTED) {
      if (task.isTimeoutElapsed(connectTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve NOT_CONNECTED within {} ms, {}",
            connectTimeout.toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve NOT_CONNECTED within timeout");
      }
    }

    // Handle backpressure
    if (result == Publication.BACK_PRESSURED) {
      if (task.isTimeoutElapsed(backpressureTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve BACK_PRESSURED within {} ms, {}",
            backpressureTimeout.toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve BACK_PRESSURED within timeout");
      }
    }

    // Handle admin action
    if (result == Publication.ADMIN_ACTION) {
      if (task.isTimeoutElapsed(adminActionTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve ADMIN_ACTION within {} ms, {}",
            adminActionTimeout.toMillis(),
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

  void close() {
    if (!eventLoop.inEventLoop()) {
      throw AeronExceptions.failWithResourceDisposal("aeron publication");
    }
    try {
      publication.close();
      logger.debug("Disposed {}", this);
    } catch (Exception ex) {
      logger.warn("{} failed on aeron.Publication close(): {}", this, ex.toString());
      throw Exceptions.propagate(ex);
    } finally {
      disposePublishTasks();
      onDispose.onComplete();
    }
  }

  /**
   * Delegates to {@link Publication#sessionId()}.
   *
   * @return aeron {@code Publication} sessionId.
   */
  int sessionId() {
    return publication.sessionId();
  }

  /**
   * Delegates to {@link Publication#isClosed()}.
   *
   * @return {@code true} if aeron {@code Publication} is closed, {@code false} otherwise
   */
  @Override
  public boolean isDisposed() {
    return publication.isClosed();
  }

  @Override
  public void dispose() {
    eventLoop
        .disposePublication(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  boolean isConnected() {
    return publication.isConnected();
  }

  private void disposePublishTasks() {
    PublishTask task;
    while ((task = publishTasks.poll()) != null) {
      try {
        task.error(AeronExceptions.failWithCancel("PublishTask has cancelled"));
      } catch (Exception ex) {
        // no-op
      }
    }
  }

  @Override
  public String toString() {
    return "MessagePublication{pub=" + publication.channel() + "}";
  }

  /**
   * Publish task.
   *
   * <p>Resident of {@link #publishTasks} queue.
   */
  private class PublishTask<B> {

    private final B buffer;
    private final DirectBufferHandler<B> bufferHandler;
    private final MonoSink<Void> sink;
    private volatile boolean isDisposed = false;

    private long start;

    /**
     * Constructor.
     *
     * @param buffer abstract buffer
     * @param bufferHandler abstract buffer handler
     * @param sink sink of result
     */
    private PublishTask(B buffer, DirectBufferHandler<B> bufferHandler, MonoSink<Void> sink) {

      this.buffer = buffer;
      this.bufferHandler = bufferHandler;
      this.sink =
          sink.onDispose(
              () -> {
                if (!isDisposed) {
                  isDisposed = true;
                  bufferHandler.dispose(buffer);
                }
              });
    }

    private long publish() {
      if (isDisposed) {
        return 1;
      }

      if (start == 0) {
        start = System.currentTimeMillis();
      }

      int length = bufferHandler.estimateLength(buffer);

      if (length < publication.maxPayloadLength()) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = publication.tryClaim(length, bufferClaim);
        if (result > 0) {
          try {
            MutableDirectBuffer directBuffer = bufferClaim.buffer();
            int offset = bufferClaim.offset();
            bufferHandler.write(directBuffer, offset, buffer, length);
            bufferClaim.commit();
          } catch (Exception ex) {
            bufferClaim.abort();
            throw Exceptions.propagate(ex);
          }
        }
        return result;
      } else {
        return publication.offer(bufferHandler.map(buffer, length));
      }
    }

    private boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void success() {
      if (!isDisposed) {
        sink.success();
      }
    }

    private void error(Throwable ex) {
      if (!isDisposed) {
        sink.error(ex);
      }
    }
  }
}
