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
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Anatoly Kadyshev
 */
public class Pooler implements Runnable {

    private final Logger logger;

    private final ExecutorService executor;

    private volatile boolean isRunning;

    private volatile InnerPooler[] poolers = new InnerPooler[0];

    public Pooler(String name) {
        this.logger = LoggerFactory.getLogger(this.getClass() + "." + name);
        this.executor = createExecutor(name);
    }

    private ExecutorService createExecutor(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, name + "-[pooler]");
            thread.setDaemon(true);
            return thread;
        });
    }

    //FIXME: Thread-safety
    public void initialise() {
        if (!isRunning) {
            isRunning = true;
            executor.submit(this);
        }
    }

    public synchronized void addSubscription(Subscription subscription, MessageHandler messageHandler) {
        FragmentAssembler handler = new FragmentAssembler(new PoolerFragmentHandler(messageHandler));
        InnerPooler data = new InnerPooler(subscription, handler, messageHandler);

        this.poolers = ArrayUtil.add(poolers, data);
    }

    public Mono<Void> shutdown() {
        return Mono.create(sink -> {
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
        logger.debug("Started");

        BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
        while (isRunning) {
            InnerPooler[] ss = poolers;
            int nReceived = 0;
            for (InnerPooler data : ss) {
                nReceived = data.poll();
            }
            idleStrategy.idle(nReceived);
        }
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

    static class InnerPooler {

        final Subscription subscription;

        final FragmentHandler handler;

        final MessageHandler messageHandler;

        InnerPooler(Subscription subscription, FragmentHandler handler, MessageHandler messageHandler) {
            this.subscription = subscription;
            this.handler = handler;
            this.messageHandler = messageHandler;
        }

        int poll() {
            int requested = (int) Math.max(messageHandler.requested(), 8);
            if (requested > 0) {
                return subscription.poll(handler, requested);
            }
            return 0;
        }

    }

}
