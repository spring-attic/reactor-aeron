package reactor.aeron;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
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

  private static ThreadFactory defaultThreadFactory() {
    return r -> {
      Thread thread = new Thread(r);
      thread.setName("aeron-event-loop");
      thread.setUncaughtExceptionHandler(
          (t, e) -> logger.error("Uncaught exception occurred: " + e));
      return thread;
    };
  }

  private Worker createWorker() {
    ThreadFactory threadFactory = defaultThreadFactory();
    Worker w = new Worker();
    thread = threadFactory.newThread(w);
    thread.start();
    return w;
  }

  boolean inEventLoop() {
    return thread == Thread.currentThread();
  }

  public Mono<Void> register(MessagePublication p) {
    return worker().flatMap(w -> command(sink -> registerPublication(p, sink)));
  }

  public Mono<Void> register(MessageSubscription s) {
    return worker().flatMap(w -> command(sink -> registerSubscription(s, sink)));
  }

  public Mono<Void> dispose(MessagePublication p) {
    return worker().flatMap(w -> command(sink -> disposePublication(p, sink)));
  }

  public Mono<Void> dispose(MessageSubscription s) {
    return worker().flatMap(w -> command(sink -> disposeSubscription(s, sink)));
  }

  private void registerPublication(MessagePublication p, MonoSink<Void> sink) {
    publications.add(p);
    sink.success();
  }

  private void registerSubscription(MessageSubscription s, MonoSink<Void> sink) {
    subscriptions.add(s);
    sink.success();
  }

  private void disposePublication(MessagePublication p, MonoSink<Void> sink) {
    publications.remove(p);
    try {
      p.close();
      sink.success();
    } catch (Exception ex) {
      sink.error(ex);
    }
  }

  private void disposeSubscription(MessageSubscription s, MonoSink<Void> sink) {
    subscriptions.remove(s);
    try {
      s.close();
      sink.success();
    } catch (Exception ex) {
      sink.error(ex);
    }
  }

  @Override
  public void dispose() {
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
          result += publications.get(i).proceed();
        }

        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = subscriptions.size(); i < n; i++) {
          result += subscriptions.get(i).poll();
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
        MessagePublication publication = it.next();
        try {
          publication.close();
        } catch (Exception ex) {
          logger.warn(
              "Exception occurred on closing publication: {}, cause: {}",
              publication,
              ex.toString());
        }
        it.remove();
      }
    }

    private void disposeSubscriptions() {
      for (Iterator<MessageSubscription> it = subscriptions.iterator(); it.hasNext(); ) {
        MessageSubscription subscription = it.next();
        try {
          subscription.close();
        } catch (Exception ex) {
          logger.warn(
              "Exception occurred on closing subscription: {}, cause: {}",
              subscription,
              ex.toString());
        }
        it.remove();
      }
    }
  }
}
