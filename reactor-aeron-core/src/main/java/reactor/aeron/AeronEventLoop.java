package reactor.aeron;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
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

  boolean inEventLoop() {
    return thread == Thread.currentThread();
  }

  /**
   * Registers message publication in event loop.
   *
   * @param p message publication
   * @return mono result of registration
   */
  public Mono<Void> register(MessagePublication p) {
    return worker().flatMap(w -> command(sink -> registerPublication(p, sink)));
  }

  /**
   * Registers message subscription in event loop.
   *
   * @param s message subscription
   * @return mono result of registration
   */
  public Mono<Void> register(MessageSubscription s) {
    return worker().flatMap(w -> command(sink -> registerSubscription(s, sink)));
  }

  /**
   * Dispose message publication and remove it from event loop.
   *
   * @param p message publication
   * @return mono result of dispose of message publication
   */
  public Mono<Void> dispose(MessagePublication p) {
    return worker().flatMap(w -> command(sink -> disposePublication(p, sink)));
  }

  /**
   * Dipose message subscription and remove it event loop.
   *
   * @param s message subscription
   * @return mono result of dispose of message subscription
   */
  public Mono<Void> dispose(MessageSubscription s) {
    return worker().flatMap(w -> command(sink -> disposeSubscription(s, sink)));
  }

  @Override
  public void dispose() {
    dispose.onComplete();

    // finish shutdown right away if no worker was created
    if (thread == null) {
      onDispose.onComplete();
    }
  }

  private void registerPublication(MessagePublication p, MonoSink<Void> sink) {
    publications.add(p);
    logger.debug("Registered publication: {}", p);
    sink.success();
  }

  private void registerSubscription(MessageSubscription s, MonoSink<Void> sink) {
    subscriptions.add(s);
    logger.debug("Registered subscription: {}", s);
    sink.success();
  }

  private void disposePublication(MessagePublication p, MonoSink<Void> sink) {
    boolean removed = publications.remove(p);
    if (removed) {
      logger.debug("Removed publication: {}", p);
    }
    try {
      p.close();
      sink.success();
    } catch (Exception ex) {
      logger.warn("Exception occurred on publication.close(): {}, cause: {}", p, ex.toString());
      sink.error(ex);
    }
  }

  private void disposeSubscription(MessageSubscription s, MonoSink<Void> sink) {
    boolean removed = subscriptions.remove(s);
    if (removed) {
      logger.debug("Removed subscription: {}", s);
    }
    try {
      s.close();
      sink.success();
    } catch (Exception ex) {
      logger.warn("Exception occurred on subscription.close(): {}, cause: {}", s, ex.toString());
      sink.error(ex);
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
    return workerMono.takeUntilOther(listenDispose());
  }

  private Mono<Void> command(Consumer<MonoSink<Void>> consumer) {
    return Mono.create(sink -> commandTasks.add(new CommandTask(sink, consumer)));
  }

  private <T> Mono<T> listenDispose() {
    //noinspection unchecked
    return dispose //
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.error(Exceptions::failWithRejected));
  }

  /**
   * Runnable task for submitting to {@link #commandTasks} queue. For usage see methods {@link
   * #register(MessagePublication)} and {@link #dispose(MessagePublication)}.
   */
  private static class CommandTask implements Runnable {
    private final MonoSink<Void> sink;
    private final Consumer<MonoSink<Void>> consumer;

    private CommandTask(MonoSink<Void> sink, Consumer<MonoSink<Void>> consumer) {
      this.sink = sink;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        consumer.accept(sink);
      } catch (Exception ex) {
        logger.warn("Exception occurred on command task: " + ex);
        sink.error(ex);
      }
    }

    private void cancel() {
      sink.error(Exceptions.failWithCancel());
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
        for (; ; ) {
          // Commands
          CommandTask task = commandTasks.poll();
          if (task == null) {
            break;
          }
          task.run();
        }

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
        disposeCommandTasks();
        disposeSubscriptions();
        disposePublications();
      } finally {
        onDispose.onComplete();
      }
    }

    private void disposeCommandTasks() {
      for (; ; ) {
        CommandTask task = commandTasks.poll();
        if (task == null) {
          break;
        }
        task.cancel();
      }
    }

    private void disposePublications() {
      for (Iterator<MessagePublication> it = publications.iterator(); it.hasNext(); ) {
        MessagePublication p = it.next();
        it.remove();
        try {
          p.close();
        } catch (Exception ex) {
          logger.warn("Exception occurred on publication.close(): {}, cause: {}", p, ex.toString());
        }
      }
    }

    private void disposeSubscriptions() {
      for (Iterator<MessageSubscription> it = subscriptions.iterator(); it.hasNext(); ) {
        MessageSubscription s = it.next();
        it.remove();
        try {
          s.close();
        } catch (Exception ex) {
          logger.warn(
              "Exception occurred on subscription.close(): {}, cause: {}", s, ex.toString());
        }
      }
    }
  }
}
