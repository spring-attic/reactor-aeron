package reactor.aeron;

import io.aeron.Publication;
import io.aeron.Subscription;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

public final class AeronEventLoop implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoop.class);

  private final AtomicInteger workerCounter = new AtomicInteger();

  private static final String workerNameFormat = "reactor-aeron-%s-%d";

  private final IdleStrategy idleStrategy;

  // Dispose signals
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  // Worker
  private final Mono<Worker> workerMono;

  // Commands
  private final Queue<CommandTask> commandTasks = new ConcurrentLinkedQueue<>();

  private final List<MessagePublication> publications = new ArrayList<>();
  private final List<MessageSubscription> subscriptions = new ArrayList<>();

  private volatile Thread thread;

  AeronEventLoop(IdleStrategy idleStrategy) {
    this.idleStrategy = idleStrategy;
    this.workerMono = Mono.fromCallable(this::createWorker).cache();
  }

  private static ThreadFactory defaultThreadFactory(String threadName) {
    return r -> {
      Thread thread = new Thread(r);
      thread.setName(threadName);
      thread.setUncaughtExceptionHandler(
          (t, e) -> logger.error("Uncaught exception occurred: " + e));
      return thread;
    };
  }

  private Worker createWorker() {
    String threadName =
        String.format(
            workerNameFormat,
            Integer.toHexString(System.identityHashCode(this)),
            workerCounter.incrementAndGet());
    ThreadFactory threadFactory = defaultThreadFactory(threadName);
    Worker w = new Worker();
    thread = threadFactory.newThread(w);
    thread.start();
    return w;
  }

  /**
   * Returns {@code true} if client called this method from within worker thread of {@link
   * AeronEventLoop}, and {@code false} otherwise.
   *
   * @return {@code true} if current thread is worker thread from event loop, and {@code false}
   *     otherwise
   */
  public boolean inEventLoop() {
    return thread == Thread.currentThread();
  }

  /**
   * Invokes factory function for aeron publication from event loop thread.
   *
   * @param supplier factory for {@link Publication} instance
   * @return mono result
   */
  public Mono<Publication> addPublication(Supplier<Publication> supplier) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        Mono.fromSupplier(supplier)
                            .subscribe(sink::success, sink::error, sink::success);
                      }
                    }));
  }

  /**
   * Invokes factory function for aeron subscription from event loop thread.
   *
   * @param supplier factory for {@link Subscription} instance
   * @return mono result
   */
  public Mono<Subscription> addSubscription(Supplier<Subscription> supplier) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        Mono.fromSupplier(supplier)
                            .subscribe(sink::success, sink::error, sink::success);
                      }
                    }));
  }

  /**
   * Registers {@link MessagePublication} in event loop.
   *
   * @param p message publication
   * @return mono result of registration
   */
  public Mono<MessagePublication> registerMessagePublication(MessagePublication p) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        publications.add(p);
                        logger.debug("Registered {}", p);
                        sink.success();
                      }
                    }));
  }

  /**
   * Registers {@link MessageSubscription} in event loop.
   *
   * @param s message subscription
   * @return mono result of registration
   */
  public Mono<MessageSubscription> registerMessageSubscription(MessageSubscription s) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        subscriptions.add(s);
                        logger.debug("Registered {}", s);
                        sink.success();
                      }
                    }));
  }

  /**
   * Disposes {@link MessagePublication} (which by turn would close aeron {@link
   * io.aeron.Publication}) and remove it from event loop.
   *
   * @param p message publication
   * @return mono result of dispose of message publication
   */
  public Mono<Void> disposeMessagePublication(MessagePublication p) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      boolean removed = publications.remove(p);
                      if (removed) {
                        logger.debug("Removed {}", p);
                      }
                      closePublication(p, sink);
                    }));
  }

  /**
   * Diposes {@link MessageSubscription} ((which by turn would close aeron {@link
   * io.aeron.Subscription})) and remove it event loop.
   *
   * @param s message subscription
   * @return mono result of dispose of message subscription
   */
  public Mono<Void> dispose(MessageSubscription s) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      boolean removed = subscriptions.remove(s);
                      if (removed) {
                        logger.debug("Removed {}", s);
                      }
                      closeSubscription(s, sink);
                    }));
  }

  @Override
  public void dispose() {
    // start disposing worker (if any)
    dispose.onComplete();

    // finish shutdown right away if no worker was created
    if (thread == null) {
      onDispose.onComplete();
    }
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  private Mono<Worker> worker() {
    return workerMono.takeUntilOther(listenUnavailable());
  }

  private <T> Mono<T> command(Consumer<MonoSink<T>> consumer) {
    return Mono.create(sink -> commandTasks.add(new CommandTask<>(sink, consumer)));
  }

  private <T> Mono<T> listenUnavailable() {
    //noinspection unchecked
    return dispose //
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.error(AeronExceptions::failWithEventLoopUnavailable));
  }

  /**
   * Runnable task for submitting to {@link #commandTasks} queue. For usage details see methods:
   * {@link #registerMessagePublication(MessagePublication)} and {@link
   * #disposeMessagePublication(MessagePublication)}.
   */
  private static class CommandTask<T> implements Runnable {
    private final MonoSink<T> sink;
    private final Consumer<MonoSink<T>> consumer;

    private CommandTask(MonoSink<T> sink, Consumer<MonoSink<T>> consumer) {
      this.sink = sink;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        consumer.accept(sink);
      } catch (Exception ex) {
        logger.warn("Exception occurred on CommandTask: " + ex);
        sink.error(ex);
      }
    }
  }

  /**
   * Runnable event loop worker. Does a following:
   *
   * <ul>
   *   <li>runs until dispose signal obtained
   *   <li>on run iteration makes progress on: a) commands; b) publications; c) subscriptions
   *   <li>idles on negative progress
   * </ul>
   */
  private class Worker implements Runnable {

    @Override
    public void run() {
      // Process commands, publications and subscriptions
      while (!dispose.isDisposed()) {

        // Commands
        processCommandTasks();

        int result = 0;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = publications.size(); i < n; i++) {
          try {
            result += publications.get(i).proceed();
          } catch (Exception ex) {
            logger.error("Unexpected exception occurred on publication.proceed(): " + ex);
          }
        }

        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = subscriptions.size(); i < n; i++) {
          try {
            result += subscriptions.get(i).poll();
          } catch (Exception ex) {
            logger.error("Unexpected exception occurred on subscription.poll(): " + ex);
          }
        }

        idleStrategy.idle(result);
      }

      // Dispose publications, subscriptions and commands
      try {
        processCommandTasks();
        disposeSubscriptions();
        disposePublications();
      } finally {
        onDispose.onComplete();
      }
    }

    private void processCommandTasks() {
      for (; ; ) {
        CommandTask task = commandTasks.poll();
        if (task == null) {
          break;
        }
        task.run();
      }
    }

    private void disposePublications() {
      for (Iterator<MessagePublication> it = publications.iterator(); it.hasNext(); ) {
        MessagePublication p = it.next();
        it.remove();
        closePublication(p);
      }
    }

    private void disposeSubscriptions() {
      for (Iterator<MessageSubscription> it = subscriptions.iterator(); it.hasNext(); ) {
        MessageSubscription s = it.next();
        it.remove();
        closeSubscription(s);
      }
    }
  }

  private void closePublication(MessagePublication p) {
    closePublication(p, null);
  }

  private void closePublication(MessagePublication p, MonoSink<Void> sink) {
    try {
      p.close();
      if (sink != null) {
        sink.success();
      }
    } catch (Exception ex) {
      logger.warn("Exception occurred on publication.close(): {}, cause: {}", p, ex.toString());
      if (sink != null) {
        sink.error(ex);
      }
    }
  }

  private void closeSubscription(MessageSubscription s) {
    closeSubscription(s, null);
  }

  private void closeSubscription(MessageSubscription s, MonoSink<Void> sink) {
    try {
      s.close();
      if (sink != null) {
        sink.success();
      }
    } catch (Exception ex) {
      logger.warn("Exception occurred on subscription.close(): {}, cause: {}", s, ex.toString());
      if (sink != null) {
        sink.error(ex);
      }
    }
  }

  private boolean cancelIfDisposed(MonoSink<?> sink) {
    boolean isDisposed = dispose.isDisposed();
    if (isDisposed) {
      sink.error(AeronExceptions.failWithCancel("CommandTask has cancelled"));
    }
    return isDisposed;
  }
}
