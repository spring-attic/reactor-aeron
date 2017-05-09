package reactor.ipc.aeron;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static reactor.ipc.aeron.TestUtils.log;

public class WriteSequencerTest {

    static class WriteSequencerForTest extends WriteSequencer<String> {

        public WriteSequencerForTest() {
            super(th -> System.err.println("Unexpected exception: " + th),
                    o -> {}, avoid -> false, null);
        }

        @Override
        protected InnerSubscriber<String> createInnerSubscriber() {
            return new SubscriberForTest(this);
        }

        void request(int n) {
            inner.request(n);
        }

        public List<Object> getSignals() {
            return ((SubscriberForTest)inner).getSignals();
        }

        static class SubscriberForTest extends InnerSubscriber<String> {

            private final List<Object> signals = new CopyOnWriteArrayList<>();

            SubscriberForTest(WriteSequencerForTest parent) {
                super(parent);
            }

            @Override
            public void doOnNext(String o) {
                log("onNext: " + o);

                signals.add(o);
            }

            @Override
            public void doOnError(Throwable t) {
                log("onError: " + t);
                promise.success();
            }

            @Override
            public void doOnComplete() {
                log("onComplete");
                promise.success();

                drainNextPublisher();
            }

            @Override
            public void onSubscribe(Subscription s) {
                log("onSubscribe: " + s);
            }

            public List<Object> getSignals() {
                return signals;
            }
        }

    }

    @Test
    public void itProvidesSignalsFromAddedPublishers() throws Exception {
        WriteSequencerForTest sequencer = new WriteSequencerForTest();
        Scheduler scheduler = Schedulers.newParallel("sequencer", 1);

        Flux<String> flux = Flux.just("Hello", "world");
        Flux<String> publishOn = flux.publishOn(Schedulers.newSingle("scheduler-1"));
        Mono result = sequencer.add(publishOn, scheduler);

        Flux<String> flux2 = Flux.just("Everybody", "happy");
        Flux<String> publishOn2 = flux2.publishOn(Schedulers.newSingle("scheduler-2"));
        Mono result2 = sequencer.add(publishOn2, scheduler);


        sequencer.request(4);
        result.block();
        result2.block();


        assertThat(sequencer.getSignals(), hasItems("Hello", "world", "Everybody", "happy"));
    }

}
