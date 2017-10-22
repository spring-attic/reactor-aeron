package reactor.ipc.aeron;

import org.junit.After;
import org.junit.Before;
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

    private Scheduler scheduler;

    private Scheduler scheduler1;

    private Scheduler scheduler2;

    @Before
    public void doSetup() {
        scheduler = Schedulers.newSingle("sequencer", false);
        scheduler1 = Schedulers.newSingle("scheduler-A");
        scheduler2 = Schedulers.newSingle("scheduler-B");
    }

    @After
    public void doTeardown() {
        scheduler.dispose();
        scheduler1.dispose();
        scheduler2.dispose();
    }

    @Test
    public void itProvidesSignalsFromAddedPublishers() throws Exception {
        WriteSequencerForTest sequencer = new WriteSequencerForTest(scheduler);

        Flux<String> flux1 = Flux.just("Hello", "world")
                .publishOn(scheduler1);
        Mono result1 = sequencer.add(flux1);

        Flux<String> flux2 = Flux.just("Everybody", "happy")
                .publishOn(scheduler2);
        Mono result2 = sequencer.add(flux2);


        sequencer.request(4);
        result1.block();
        result2.block();


        assertThat(sequencer.getSignals(), hasItems("Hello", "world", "Everybody", "happy"));
    }

    static class WriteSequencerForTest extends WriteSequencer<String> {

        static final Consumer<Throwable> ERROR_HANDLER = th -> System.err.println("Unexpected exception: " + th);

        private final SubscriberForTest inner;

        WriteSequencerForTest(Scheduler scheduler) {
            super(scheduler, discardedValue -> {});
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
            void doOnSubscribe() {
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

                scheduleNextPublisherDrain();
            }

        }

    }

}
