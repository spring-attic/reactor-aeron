package reactor.ipc.aeron;

import org.junit.Test;
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

    static class WriteSequencerForTest extends WriteSequencer<String> {

        static final Consumer<Throwable> ERROR_HANDLER = th -> System.err.println("Unexpected exception: " + th);

        private final SubscriberForTest inner;

        public WriteSequencerForTest() {
            super(o -> {}, avoid -> false, null);
            this.inner = new SubscriberForTest(this);
        }

        @Override
        InnerSubscriber<String> getInner() {
            return inner;
        }

        @Override
        Consumer<Throwable> getErrorHandler() {
            return ERROR_HANDLER;
        }

        void request(int n) {
            inner.request(n);
        }

        List<String> getSignals() {
            return inner.signals;
        }

        static class SubscriberForTest extends InnerSubscriber<String> {

            final List<String> signals = new CopyOnWriteArrayList<>();

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

        }

    }

}
