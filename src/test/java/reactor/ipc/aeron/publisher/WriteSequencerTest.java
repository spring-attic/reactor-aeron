package reactor.ipc.aeron.publisher;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static reactor.ipc.aeron.TestUtils.log;

public class WriteSequencerTest {

    @Test
    public void itProvidesSignalsFromAddedPublishers() throws Exception {
        Consumer<Throwable> errorHandler = th -> System.err.println("Unexpected exception: " + th);
        SubscriberForTest subscriber = new SubscriberForTest();
        WriteSequencer<Object> publisher = new WriteSequencer<>(subscriber, errorHandler, o -> {}, aVoid -> false, null);
        Scheduler scheduler = Schedulers.newParallel("sequencer", 1);

        Flux<String> flux = Flux.just("Hello", "world");
        Flux<String> publishOn = flux.publishOn(Schedulers.newSingle("scheduler-1"));
        Mono result = publisher.add(publishOn, scheduler);

        Flux<String> flux2 = Flux.just("Everybody", "happy");
        Flux<String> publishOn2 = flux2.publishOn(Schedulers.newSingle("scheduler-2"));
        Mono result2 = publisher.add(publishOn2, scheduler);


        subscriber.request(4);
        result.block();
        result2.block();


        assertThat(subscriber.getSignals(), hasItems("Hello", "world", "Everybody", "happy"));
    }

    private static class SubscriberForTest implements Subscriber<Object> {

        private WriteSequencerSubscription s;

        private final List<Object> signals = new CopyOnWriteArrayList<>();

        @Override
        public void onNext(Object o) {
            log("onNext: " + o);

            signals.add(o);
        }

        @Override
        public void onError(Throwable t) {
            log("onError: " + t);
            s.getPromise().success();
        }

        @Override
        public void onComplete() {
            log("onComplete");
            s.getPromise().success();

            s.drainNextPublisher();
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = (WriteSequencerSubscription) s;
            log("onSubscribe: " + s);
        }

        public void request(long n) {
            s.request(n);
        }

        public List<Object> getSignals() {
            return signals;
        }
    }

}
