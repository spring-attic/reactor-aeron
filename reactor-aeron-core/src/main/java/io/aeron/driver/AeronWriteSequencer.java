package io.aeron.driver;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.ipc.aeron.AbortedException;
import reactor.ipc.aeron.MessagePublication;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;

public class AeronWriteSequencer {

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<AeronWriteSequencer> WIP =
      AtomicIntegerFieldUpdater.newUpdater(AeronWriteSequencer.class, "wip");

  private static final Logger logger = Loggers.getLogger(AeronWriteSequencer.class);

  private final PublisherSender inner;

  private final Consumer<Throwable> errorHandler;

  // Cast the supplied queue (SpscLinkedArrayQueue) to use its atomic dual-insert backed by {@link
  // BiPredicate#test)
  private final BiPredicate<MonoSink<?>, Object> pendingWriteOffer;
  private final Queue<?> pendingWrites;
  private final Consumer<Object> discardedHandler;
  private final Scheduler scheduler;

  @SuppressWarnings("unused")
  private volatile int wip;

  AeronWriteSequencer(
      Scheduler scheduler, String category, MessagePublication publication, long sessionId) {
    this.discardedHandler =
        o -> {
          // no-op
        };
    this.scheduler = scheduler;
    this.pendingWrites = Queues.unbounded().get();
    //noinspection unchecked
    this.pendingWriteOffer = (BiPredicate<MonoSink<?>, Object>) pendingWrites;
    this.errorHandler = th -> logger.error("[{}] Unexpected exception", category, th);
    this.inner = new PublisherSender(this, publication, sessionId);
  }

  Consumer<Throwable> getErrorHandler() {
    return errorHandler;
  }

  PublisherSender getInner() {
    return inner;
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencer} instance.
   *
   * @param publisher data publisher
   * @return mono handle
   */
  public Mono<Void> add(Publisher<?> publisher) {
    return Mono.create(
        sink -> {
          boolean success = pendingWriteOffer.test(sink, publisher);
          if (!success) {
            sink.error(new Exception("Failed to enqueue publisher"));
          }

          scheduleDrain();
        });
  }

  boolean isReady() {
    return !getInner().isCancelled();
  }

  private void drain() {
    PublisherSender inner = getInner();
    if (WIP.getAndIncrement(this) == 0) {

      for (; ; ) {
        if (inner.isCancelled()) {
          discard();

          inner.setCancelled(false);

          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        if (inner.isActive()) {
          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        MonoSink<?> promise;
        Object v = pendingWrites.poll();

        try {
          promise = (MonoSink<?>) v;
        } catch (Throwable e) {
          getErrorHandler().accept(e);
          return;
        }

        boolean empty = promise == null;

        if (empty) {
          if (WIP.decrementAndGet(this) == 0) {
            break;
          }
          continue;
        }

        v = pendingWrites.poll();
        //noinspection unchecked
        Publisher<ByteBuffer> p = (Publisher<ByteBuffer>) v;

        if (p instanceof Callable) {
          @SuppressWarnings("unchecked")
          Callable<?> supplier = (Callable<?>) p;

          Object vr;

          try {
            vr = supplier.call();
          } catch (Throwable e) {
            promise.error(e);
            continue;
          }

          if (vr == null) {
            promise.success();
            continue;
          }

          inner.setActive(true);
          inner.setResultSink(promise);
          inner.onSubscribe(Operators.scalarSubscription(inner, (ByteBuffer) vr));
        } else {
          inner.setActive(true);
          inner.setResultSink(promise);
          //noinspection ConstantConditions
          p.subscribe(inner);
        }
      }
    }
  }

  void discard() {
    while (!pendingWrites.isEmpty()) {
      Object v = pendingWrites.poll();
      MonoSink<?> promise;
      try {
        promise = (MonoSink<?>) v;
      } catch (Throwable e) {
        getErrorHandler().accept(e);
        return;
      }
      v = pendingWrites.poll();
      if (logger.isDebugEnabled()) {
        logger.debug("Terminated. Dropping: {}", v);
      }

      discardedHandler.accept(v);

      promise.error(new AbortedException());
    }
  }

  void scheduleDrain() {
    scheduler.schedule(this::drain);
  }
}
