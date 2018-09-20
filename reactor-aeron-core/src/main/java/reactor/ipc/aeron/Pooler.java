/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.ipc.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;

/** @author Anatoly Kadyshev */
public class Pooler implements Runnable {

  private static final Logger logger = Loggers.getLogger(Pooler.class);

  private final String name;

  private final ExecutorService executor;

  private volatile boolean isRunning;

  private volatile InnerPooler[] poolers = new InnerPooler[0];

  public Pooler(String name) {
    this.name = name;
    this.executor = createExecutor(name);
  }

  private ExecutorService createExecutor(String name) {
    return Executors.newSingleThreadExecutor(
        r -> {
          Thread thread = new Thread(r, name + "-[pooler]");
          thread.setDaemon(true);
          return thread;
        });
  }

  // FIXME: Thread-safety
  public void initialise() {
    if (!isRunning) {
      isRunning = true;
      executor.submit(this);
    }
  }

  public void addControlSubscription(
      Subscription subscription, ControlMessageSubscriber subscriber) {
    InnerPooler pooler = new InnerPooler(subscription, subscriber);
    addPooler(pooler);
  }

  public void addDataSubscription(Subscription subscription, DataMessageSubscriber subscriber) {
    InnerPooler pooler = new InnerPooler(subscription, subscriber);
    addPooler(pooler);
  }

  private synchronized void addPooler(InnerPooler pooler) {
    this.poolers = ArrayUtil.add(poolers, pooler);
  }

  public Mono<Void> shutdown() {
    return Mono.create(
        sink -> {
          isRunning = false;
          AtomicBoolean shouldRetry = new AtomicBoolean(true);
          sink.onCancel(() -> shouldRetry.set(false));
          executor.shutdown();
          try {
            while (shouldRetry.get()) {
              boolean isTerminated = executor.awaitTermination(1, TimeUnit.SECONDS);
              if (isTerminated) {
                break;
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          sink.success();

          logger.debug("Terminated");
        });
  }

  @Override
  public void run() {
    logger.debug("[{}] Started", name);

    BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
    while (isRunning) {
      InnerPooler[] ss = poolers;
      int nReceived = 0;
      for (InnerPooler data : ss) {
        nReceived = data.poll();
      }
      idleStrategy.idle(nReceived);
    }

    logger.debug("[{}] Terminated", name);
  }

  public synchronized void removeSubscription(Subscription subscription) {
    InnerPooler[] ss = poolers;
    for (int i = 0; i < ss.length; i++) {
      InnerPooler pooler = ss[i];
      if (pooler.subscription == subscription) {
        this.poolers = ArrayUtil.remove(poolers, i);
        break;
      }
    }
  }

  static class InnerPooler implements org.reactivestreams.Subscription {

    final Subscription subscription;

    final FragmentHandler handler;

    volatile long requested = 0;

    private static final AtomicLongFieldUpdater<InnerPooler> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(InnerPooler.class, "requested");

    InnerPooler(Subscription subscription, ControlMessageSubscriber subscriber) {
      this(subscription, subscriber, new ControlPoolerFragmentHandler(subscriber));
    }

    InnerPooler(Subscription subscription, DataMessageSubscriber subscriber) {
      this(subscription, subscriber, new DataPoolerFragmentHandler(subscriber));
    }

    private InnerPooler(
        Subscription subscription, PoolerSubscriber subscriber, FragmentHandler handler) {
      this.subscription = subscription;
      this.handler = new FragmentAssembler(handler);

      subscriber.onSubscribe(this);
    }

    int poll() {
      int r = (int) Math.min(requested, 8);
      int nPolled = 0;
      if (r > 0) {
        nPolled = subscription.poll(handler, r);
        if (nPolled > 0) {
          Operators.produced(REQUESTED, this, nPolled);
        }
      }
      return nPolled;
    }

    @Override
    public void request(long n) {
      Operators.addCap(REQUESTED, this, n);
    }

    @Override
    public void cancel() {}
  }
}
