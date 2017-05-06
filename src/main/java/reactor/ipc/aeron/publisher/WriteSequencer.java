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
package reactor.ipc.aeron.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.QueueSupplier;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class WriteSequencer<T> {

    private static final Logger log = Loggers.getLogger(WriteSequencer.class);
    /**
     * Cast the supplied queue (SpscLinkedArrayQueue) to use its atomic dual-insert
     * backed by {@link BiPredicate#test)
     **/
    private final BiPredicate<MonoSink<?>, Object> pendingWriteOffer;

    private final Queue<?> pendingWrites;

    private volatile int     wip;

    // a publisher is being drained
    private volatile boolean innerActive;

    private final InnerSubscriber inner;

    private final Consumer<Throwable> errorHandler;

    private final Consumer<Object> discardedHandler;

    private final Predicate<Void> backpressureChecker;

    private final BiConsumer<Object, MonoSink<?>> messageConsumer;

    @SuppressWarnings("unchecked")
    public WriteSequencer(Subscriber<T> delegate,
                          Consumer<Throwable> errorHandler,
                          Consumer<Object> discardedHandler,
                          Predicate<Void> backpressureChecker,
                          BiConsumer<Object, MonoSink<?>> messageConsumer) {
        this.inner = new InnerSubscriber<>(this, delegate);
        this.errorHandler = errorHandler;
        this.discardedHandler = discardedHandler;
        this.backpressureChecker = backpressureChecker;
        this.pendingWrites = QueueSupplier.unbounded()
                .get();
        this.pendingWriteOffer = (BiPredicate<MonoSink<?>, Object>) pendingWrites;
        this.messageConsumer = messageConsumer;
    }

    public Mono<Void> add(Object msg, Scheduler scheduler) {
        if (!(msg instanceof Publisher) && messageConsumer == null) {
            return Mono.error(new IllegalArgumentException("Cannot add msg of class " + msg.getClass()));
        }
        else {
            return Mono.create(sink -> {
                boolean success = pendingWriteOffer.test(sink, msg);
                if (!success) {
                    sink.error(new Exception("Failed to enqueue publisher"));
                }

                scheduler.schedule(this::drain);
            });
        }
    }

    public boolean isEmpty() {
        return pendingWrites.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public void drain() {
        if (WIP.getAndIncrement(this) == 0) {

            for ( ; ; ) {
                if (inner.isCancelled) {
                    discard();
                    return;
                }

                if (innerActive || backpressureChecker.test(null)) {
                    if (WIP.decrementAndGet(this) == 0) {
                        break;
                    }
                    continue;
                }

                MonoSink<?> promise;
                Object v = pendingWrites.poll();

                try {
                    promise = (MonoSink<?>) v;
                }
                catch (Throwable e) {
                    errorHandler.accept(e);
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

                if (v instanceof Publisher) {
                    Publisher<?> p = (Publisher<?>) v;

                    if (p instanceof Callable) {
                        @SuppressWarnings("unchecked") Callable<?> supplier = (Callable<?>) p;

                        Object vr;

                        try {
                            vr = supplier.call();
                        }
                        catch (Throwable e) {
                            promise.error(e);
                            continue;
                        }

                        if (vr == null) {
                            promise.success();
                            continue;
                        }

                        if (inner.unbounded && messageConsumer != null) {
                            messageConsumer.accept(vr, promise);
                        }
                        else {
                            innerActive = true;
                            inner.setResultSink(promise);
                            inner.onSubscribe(Operators.scalarSubscription(inner, vr));
                        }
                    }
                    else {
                        innerActive = true;
                        inner.setResultSink(promise);
                        p.subscribe(inner);
                    }
                }
                else {
                    messageConsumer.accept(v, promise);
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
            }
            catch (Throwable e) {
                errorHandler.accept(e);
                return;
            }
            v = pendingWrites.poll();
            if (log.isDebugEnabled()) {
                log.debug("Terminated. Dropping: {}", v);
            }

            discardedHandler.accept(v);

            promise.error(new AbortedException());
        }
    }

    static final class InnerSubscriber<T>
            implements Subscriber<T>, WriteSequencerSubscription {

        final WriteSequencer<T> parent;

        final Subscriber<T> delegate;

        volatile Subscription missedSubscription;
        volatile long         missedRequested;
        volatile long         missedProduced;
        volatile int          wip;

        // a subscription has been cancelled
        volatile boolean isCancelled;

        /**
         * The current outstanding request amount.
         */
        long           requested;
        boolean        unbounded;

        /**
         * The current subscription which may null if no Subscriptions have been set.
         */
        Subscription   actual;
        long           produced;
        MonoSink<?>    promise;

        InnerSubscriber(WriteSequencer<T> parent, Subscriber<T> delegate) {
            this.parent = parent;
            this.delegate = delegate;
            delegate.onSubscribe(this);
        }

        public void setResultSink(MonoSink<?> promise) {
            this.promise = promise;
        }

        @Override
        public final void cancel() {
            if (!isCancelled) {
                isCancelled = true;

                drain();
            }
        }

        @Override
        public void onComplete() {
            long p = produced;
            parent.innerActive = false;

            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            delegate.onComplete();
        }

        @Override
        public void onError(Throwable t) {
            long p = produced;
            parent.innerActive = false;

            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            delegate.onError(t);
        }

        @Override
        public void onNext(T t) {
            produced++;

            delegate.onNext(t);
        }

        @Override
        public void onSubscribe(Subscription s) {
            Objects.requireNonNull(s);

            if (isCancelled) {
                s.cancel();
                return;
            }

            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                actual = s;

                long r = requested;

                if (WIP.decrementAndGet(this) != 0) {
                    drainLoop();
                }

                if (r != 0L) {
                    s.request(r);
                }

                return;
            }

            MISSED_SUBSCRIPTION.set(this, s);
            drain();
        }

        @Override
        public final void request(long n) {
            if (Operators.validate(n)) {
                if (unbounded) {
                    return;
                }
                if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                    long r = requested;

                    if (r != Long.MAX_VALUE) {
                        r = Operators.addCap(r, n);
                        requested = r;
                        if (r == Long.MAX_VALUE) {
                            unbounded = true;
                        }
                    }
                    Subscription a = actual;

                    if (WIP.decrementAndGet(this) != 0) {
                        drainLoop();
                    }

                    if (a != null) {
                        a.request(n);
                    }

                    return;
                }

                Operators.getAndAddCap(MISSED_REQUESTED, this, n);

                drain();
            }
        }

        final void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            drainLoop();
        }

        final void drainLoop() {
            int missed = 1;

            long requestAmount = 0L;
            Subscription requestTarget = null;

            for (; ; ) {

                Subscription ms = missedSubscription;

                if (ms != null) {
                    ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
                }

                long mr = missedRequested;
                if (mr != 0L) {
                    mr = MISSED_REQUESTED.getAndSet(this, 0L);
                }

                long mp = missedProduced;
                if (mp != 0L) {
                    mp = MISSED_PRODUCED.getAndSet(this, 0L);
                }

                Subscription a = actual;

                if (isCancelled) {
                    if (a != null) {
                        a.cancel();
                        actual = null;
                    }
                    if (ms != null) {
                        ms.cancel();
                    }
                }
                else {
                    long r = requested;
                    if (r != Long.MAX_VALUE) {
                        long u = Operators.addCap(r, mr);

                        if (u != Long.MAX_VALUE) {
                            long v = u - mp;
                            if (v < 0L) {
                                Operators.reportMoreProduced();
                                v = 0;
                            }
                            r = v;
                        }
                        else {
                            r = u;
                        }
                        requested = r;
                    }

                    if (ms != null) {
                        actual = ms;
                        if (r != 0L) {
                            requestAmount = Operators.addCap(requestAmount, r);
                            requestTarget = ms;
                        }
                    }
                    else if (mr != 0L && a != null) {
                        requestAmount = Operators.addCap(requestAmount, mr);
                        requestTarget = a;
                    }
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    if (requestAmount != 0L) {
                        requestTarget.request(requestAmount);
                    }
                    return;
                }
            }
        }

        final void produced(long n) {
            if (unbounded) {
                return;
            }
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;

                if (r != Long.MAX_VALUE) {
                    long u = r - n;
                    if (u < 0L) {
                        Operators.reportMoreProduced();
                        u = 0;
                    }
                    requested = u;
                }
                else {
                    unbounded = true;
                }

                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }

                drainLoop();

                return;
            }

            Operators.getAndAddCap(MISSED_PRODUCED, this, n);

            drain();
        }

        @Override
        public long getProduced() {
            return produced;
        }

        @Override
        public MonoSink<?> getPromise() {
            return promise;
        }

        @Override
        public void drainNextPublisher() {
            parent.drain();
        }

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<InnerSubscriber, Subscription>
                MISSED_SUBSCRIPTION =
                AtomicReferenceFieldUpdater.newUpdater(InnerSubscriber.class,
                        Subscription.class,
                        "missedSubscription");
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<InnerSubscriber> MISSED_REQUESTED    =
                AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class,
                        "missedRequested");
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<InnerSubscriber>    MISSED_PRODUCED     =
                AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class,
                        "missedProduced");

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<InnerSubscriber> WIP                 =
                AtomicIntegerFieldUpdater.newUpdater(InnerSubscriber.class, "wip");
    }

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<WriteSequencer> WIP =
            AtomicIntegerFieldUpdater.newUpdater(WriteSequencer.class, "wip");

}
