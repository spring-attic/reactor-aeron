package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SignalType;

class MessagePublication implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private static final AtomicReferenceFieldUpdater<MessagePublication, PublisherProcessor[]>
      PUBLISHER_PROCESSORS =
          AtomicReferenceFieldUpdater.newUpdater(
              MessagePublication.class, PublisherProcessor[].class, "publisherProcessors");

  private final Publication publication;
  private final AeronEventLoop eventLoop;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration adminActionTimeout;

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private volatile PublisherProcessor[] publisherProcessors = new PublisherProcessor[0];

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

  private static class PublisherProcessor extends BaseSubscriber {

    private final DirectBufferHandler<?> bufferHandler;
    private final MessagePublication messagePublication;

    PublisherProcessor(
        DirectBufferHandler<?> bufferHandler, MessagePublication messagePublication) {
      this.bufferHandler = bufferHandler;
      this.messagePublication = messagePublication;

      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = messagePublication.publisherProcessors;
        newArray = ArrayUtil.add(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(messagePublication, oldArray, newArray));
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      // no-op
    }

    @Override
    protected void hookOnNext(Object value) {
      // todo
    }

    @Override
    protected void hookFinally(SignalType type) {
      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = messagePublication.publisherProcessors;
        newArray = ArrayUtil.remove(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(messagePublication, oldArray, newArray));
    }
  }

  /**
   * Enqueues publisher for future sending.
   *
   * @param publisher abstract publisher to process messages from
   * @param bufferHandler abstract buffer handler
   * @return mono handle
   */
  <B> Mono<Void> publish(Publisher<B> publisher, DirectBufferHandler<? super B> bufferHandler) {
    return Mono.<Void>fromRunnable(
            () -> publisher.subscribe(new PublisherProcessor(bufferHandler, this)))
        .takeUntilOther(onPublicationDispose());
  }

  private Mono<Void> onPublicationDispose() {
    return onDispose()
        .then(Mono.defer(() -> Mono.error(AeronExceptions.failWithPublicationUnavailable())));
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return more than or equal {@code 1} - some progress was done; {@code 0} - denotes no progress
   *     was done
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

  /**
   * Spins (in async fashion) until {@link Publication#isConnected()} would have returned {@code
   * true} or {@code connectTimeout} elapsed. See also {@link
   * MessagePublication#ensureConnected0()}.
   *
   * @return mono result
   */
  Mono<MessagePublication> ensureConnected() {
    return Mono.defer(
        () -> {
          Duration retryInterval = Duration.ofMillis(100);
          long retryCount = Math.max(connectTimeout.toMillis() / retryInterval.toMillis(), 1);

          return ensureConnected0()
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .doOnError(
                  ex -> logger.warn("aeron.Publication is not connected after several retries"))
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

  private static class PublishTask<B> {

    private static final ThreadLocal<BufferClaim> bufferClaims =
        ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private B buffer;
    private DirectBufferHandler<B> bufferHandler;
    private MonoProcessor<Void> promise;

    private long start;

    private static <B> PublishTask<B> newInstance(
        Publication publication,
        B buffer,
        DirectBufferHandler<B> bufferHandler,
        MonoProcessor<Void> promise) {

      PublishTask<B> task = new PublishTask<>();

      task.publication = publication;
      task.buffer = buffer;
      task.bufferHandler = bufferHandler;
      task.start = 0;
      task.promise = promise;
      return task;
    }

    private long publish() {
      if (promise.isDisposed()) {
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
      promise.onComplete();
      bufferHandler.dispose(buffer);
    }

    private void error(Throwable ex) {
      promise.onError(ex);
      bufferHandler.dispose(buffer);
    }
  }
}
