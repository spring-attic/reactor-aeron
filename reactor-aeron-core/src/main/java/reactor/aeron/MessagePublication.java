package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Queue;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

public final class MessagePublication implements OnDisposable {

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
   * @param publication publication
   * @param options aeron options
   */
  MessagePublication(Publication publication, AeronEventLoop eventLoop, AeronOptions options) {
    this.publication = publication;
    this.eventLoop = eventLoop;
    this.connectTimeout = options.connectTimeout();
    this.backpressureTimeout = options.backpressureTimeout();
    this.adminActionTimeout = options.adminActionTimeout();
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

  /** TODO wrote sometghign meaninglful */
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

  /**
   * Delegates to {@link Publication#sessionId()}
   *
   * @return aeron {@code Publication} sessionId.
   */
  public int sessionId() {
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

  /**
   * Spins (in async fashion) until {@link Publication#isConnected()} would have returned {@code
   * true} or {@code connectTimeout} elapsed. See also {@link
   * MessagePublication#ensureConnected0()}.
   *
   * @return mono result
   */
  public Mono<MessagePublication> ensureConnected() {
    return Mono.defer(
        () -> {
          Duration retryInterval = Duration.ofMillis(100);
          long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();
          retryCount = Math.max(retryCount, 1);

          return ensureConnected0()
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .timeout(connectTimeout)
              .doOnError(
                  ex -> logger.warn("Failed to connect publication: {}", publication.channel()))
              .thenReturn(this);
        });
  }

  private Mono<Void> ensureConnected0() {
    return Mono.defer(
        () ->
            publication.isConnected()
                ? Mono.empty()
                : Mono.error(
                    AeronExceptions.failWithPublication("aeron.Publication is not connected")));
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
    return ""; // TODO implement
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
