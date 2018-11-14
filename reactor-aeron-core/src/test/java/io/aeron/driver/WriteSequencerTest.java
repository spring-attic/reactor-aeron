package io.aeron.driver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static reactor.ipc.aeron.TestUtils.log;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.MessagePublication;

public class WriteSequencerTest {

  private Scheduler scheduler;

  private Scheduler scheduler1;

  private Scheduler scheduler2;
  private BlockingQueue<Runnable> commandQueue;

  /** Setup. */
  @BeforeEach
  public void doSetup() {
    commandQueue = new LinkedBlockingQueue<>();

    ThreadPoolExecutor executorService =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, commandQueue);
    executorService.prestartAllCoreThreads();
    scheduler = Schedulers.fromExecutorService(executorService);

    scheduler1 = Schedulers.newSingle("scheduler-A");
    scheduler2 = Schedulers.newSingle("scheduler-B");
  }

  /** Teardown. */
  @AfterEach
  public void doTeardown() {
    commandQueue.clear();
    scheduler.dispose();
    scheduler1.dispose();
    scheduler2.dispose();
  }

  @Test
  public void itProvidesSignalsFromAddedPublishers() throws Exception {

    WriteSequencerForTest sequencer = new WriteSequencerForTest(commandQueue);

    Flux<ByteBuffer> flux1 =
        Flux.just("Hello", "world").map(AeronUtils::stringToByteBuffer).publishOn(scheduler1);
    Mono result1 = sequencer.add(flux1);

    Flux<ByteBuffer> flux2 =
        Flux.just("Everybody", "happy").map(AeronUtils::stringToByteBuffer).publishOn(scheduler2);
    Mono result2 = sequencer.add(flux2);

    result1.block();
    result2.block();

    assertThat(sequencer.getSignals(), hasItems("Hello", "world", "Everybody", "happy"));
  }

  static class WriteSequencerForTest extends AeronWriteSequencer {

    static final Consumer<Throwable> ERROR_HANDLER =
        th -> System.err.println("Unexpected exception: " + th);

    private final SubscriberForTest inner;

    WriteSequencerForTest(Queue<Runnable> queue) {
      super(queue, "test", null, 1234);
      this.inner = new SubscriberForTest(this, null, 1234);
    }

    @Override
    PublisherSender getInner() {
      return inner;
    }

    @Override
    Consumer<Throwable> getErrorHandler() {
      return ERROR_HANDLER;
    }

    List<String> getSignals() {
      return inner
          .signals
          .stream()
          .map(AeronUtils::byteBufferToString)
          .collect(Collectors.toList());
    }

    static class SubscriberForTest extends PublisherSender {

      final List<ByteBuffer> signals = new CopyOnWriteArrayList<>();

      SubscriberForTest(
          AeronWriteSequencer sequencer, MessagePublication publication, long sessionId) {
        super(sequencer, publication, sessionId);
      }

      public void doOnNext(ByteBuffer o) {
        log("onNext: " + o);
        signals.add(o);
      }

      void doOnSubscribe() {
        request(Long.MAX_VALUE);
      }

      public void doOnError(Throwable t) {
        log("onError: " + t);
        super.doOnError(t);
      }

      public void doOnComplete() {
        log("onComplete");
        super.doOnComplete();
      }
    }
  }
}
