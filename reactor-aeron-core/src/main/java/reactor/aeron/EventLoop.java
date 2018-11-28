package reactor.aeron;

import io.aeron.Publication;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

final class EventLoop implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(EventLoop.class);

  // Default thread factory
  private static final ThreadFactory THREAD_FACTORY = defaultThreadFactory();

  // Dispose signals
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  // Worker
  private final Mono<Thread> workerMono;

  // Commands
  private final Queue<Runnable> cmdQueue = new ConcurrentLinkedQueue<>();

  // Senders
  private final List<MessagePublication> messagePublications = new ArrayList<>();

  /**
   * Default constructor.
   *
   * <p>
   */
  EventLoop() {
    dispose //
        .doOnTerminate(this::dispose0)
        .subscribe();

    workerMono =
        Mono.fromCallable(
                () -> {
                  Thread thread = THREAD_FACTORY.newThread(new Worker());
                  thread.start();
                  return thread;
                })
            .cache();
  }

  Mono<Void> register(MessagePublication messagePublication) {
    return workerMono
        .takeUntilOther(listenDisposed())
        .flatMap(t -> newCommand(sink -> register0(messagePublication, sink)));
  }

  Mono<Void> unregister(MessagePublication messagePublication) {
    return workerMono
        .takeUntilOther(listenDisposed())
        .flatMap(t -> newCommand(sink -> unregister0(messagePublication, sink)));
  }

  Mono<Void> close(Publication publication) {
    return workerMono
        .takeUntilOther(listenDisposed())
        .flatMap(t -> newCommand(sink -> close0(publication, sink)));
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  private <T> Mono<T> listenDisposed() {
    //noinspection unchecked
    return onDispose
        .map(avoid -> (T) avoid)
        .switchIfEmpty(
            Mono.error(() -> new RejectedExecutionException("aeron-event-loop disposed")));
  }

  private static ThreadFactory defaultThreadFactory() {
    return r -> {
      Thread thread = new Thread(r);
      thread.setName("aeron-event-loop");
      thread.setUncaughtExceptionHandler(
          (t, e) -> logger.error("Uncaught exception occurred: {}", e, e));
      return thread;
    };
  }

  private Mono<Void> newCommand(Consumer<MonoSink<Void>> consumer) {
    return Mono.create(sink -> cmdQueue.add(() -> consumer.accept(sink)));
  }

  private void register0(MessagePublication messagePublication, MonoSink<Void> sink) {
    if (messagePublication == null) {
      sink.error(new IllegalArgumentException("messagePublication must be not null"));
    } else {
      messagePublications.add(messagePublication);
      sink.success();
    }
  }

  private void unregister0(MessagePublication messagePublication, MonoSink<Void> sink) {
    messagePublications.removeIf(p -> p == messagePublication);
    sink.success();
  }

  private void close0(Publication publication, MonoSink<Void> sink) {
    try {
      Optional.ofNullable(publication).ifPresent(Publication::close);
      sink.success();
    } catch (Exception ex) {
      logger.warn("Exception occurred on closing publication, cause: {}", ex);
      sink.error(ex);
    }
  }

  private void dispose0() {}

  private class Worker implements Runnable {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {}
    }
  }
}
