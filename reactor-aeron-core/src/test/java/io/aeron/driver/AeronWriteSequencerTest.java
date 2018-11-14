package io.aeron.driver;

import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.Condition;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.MessageType;
import reactor.test.StepVerifier;

public class AeronWriteSequencerTest {

  private Scheduler scheduler;

  private static final Duration TIMEOUT = Duration.ofSeconds(1);
  private BlockingQueue<Runnable> commandQueue;

  /** Setup. */
  @BeforeEach
  public void doSetup() {
    commandQueue = new LinkedBlockingQueue<>();
    ThreadPoolExecutor executorService =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, commandQueue);
    executorService.prestartAllCoreThreads();
    scheduler = Schedulers.fromExecutorService(executorService);
  }

  /** Teardown. */
  @AfterEach
  public void doTeardown() {
    commandQueue.clear();
    scheduler.dispose();
  }

  @Test
  public void itSendsPublishers() {
    FakeMessagePublication publication = new FakeMessagePublication();
    publication.publishSuccessfully(4);

    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(commandQueue, "test", publication, 1);

    Mono<Void> result1 = writeSequencer.add(ByteBufferFlux.from("Hello", "world"));
    Mono<Void> result2 = writeSequencer.add(ByteBufferFlux.from("All", "good"));

    StepVerifier.create(result1).expectComplete().verify(TIMEOUT);

    StepVerifier.create(result2).expectComplete().verify(TIMEOUT);

    StepVerifier.create(publication.messages().asString())
        .expectNext("Hello", "world", "All", "good")
        .thenCancel()
        .verify(TIMEOUT);
  }

  @Test
  public void itSendsNewlyAddedPublisherAfterCurrentIsBackpressured() {
    FakeMessagePublication publication = new FakeMessagePublication();
    publication.publishSuccessfully(3);
    publication.failPublication(Publication.BACK_PRESSURED);
    publication.publishSuccessfully(2);

    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(commandQueue, "test", publication, 1);

    Mono<Void> result1 = writeSequencer.add(ByteBufferFlux.from("Hello", "world"));
    Mono<Void> result2 = writeSequencer.add(ByteBufferFlux.from("1", "2"));

    StepVerifier.create(result1).expectComplete().verify();
    StepVerifier.create(result2).expectError().verify();

    new Condition(writeSequencer::isReady).awaitTrue(TIMEOUT);

    Mono<Void> result3 = writeSequencer.add(ByteBufferFlux.from("After", "error"));

    StepVerifier.create(result3).expectComplete().verify(TIMEOUT);
  }

  @Test
  public void itSendsNextEnqueuedPublisherAfterCurrentSignalsComplete() {
    FakeMessagePublication publication = new FakeMessagePublication();
    publication.publishSuccessfully(4);

    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(commandQueue, "test", publication, 1);

    Scheduler scheduler = Schedulers.single();
    writeSequencer.add(ByteBufferFlux.from("Hello", "world").subscribeOn(scheduler)).subscribe();
    writeSequencer.add(ByteBufferFlux.from("I'm", "here").subscribeOn(scheduler)).subscribe();

    StepVerifier.create(publication.messages().asString())
        .expectNext("Hello", "world", "I'm", "here")
        .expectNoEvent(Duration.ofMillis(250))
        .thenCancel()
        .verify(TIMEOUT);
  }

  @Test
  public void itSendsNextEnqueuedPublisherAfterCurrentSignalsError() {
    FakeMessagePublication publication = new FakeMessagePublication();
    publication.publishSuccessfully(4);

    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(commandQueue, "test", publication, 1);

    Scheduler scheduler = Schedulers.single();
    Mono<Void> result1 =
        writeSequencer.add(
            Flux.merge(
                    ByteBufferFlux.from("Hello", "world"),
                    Flux.error(new Exception("Publisher failed")))
                .subscribeOn(scheduler));

    StepVerifier.create(result1).expectError().verify(TIMEOUT);

    writeSequencer
        .add(ByteBufferFlux.from("I'm", "here").subscribeOn(scheduler))
        .subscribe(
            msg -> {
              // no-op
            },
            th -> {
              // no-op
            });

    StepVerifier.create(publication.messages().asString())
        .expectNext("Hello", "world", "I'm", "here")
        .expectNoEvent(Duration.ofMillis(250))
        .thenCancel()
        .verify(TIMEOUT);
  }

  @Test
  public void testItRequestsDataByBatches() {
    FakeMessagePublication publication = new FakeMessagePublication();
    publication.publishSuccessfully(32);

    AeronWriteSequencer writeSequencer =
        new AeronWriteSequencer(commandQueue, "test", publication, 1);

    Mono<Void> result =
        writeSequencer.add(
            Flux.range(1, 32).map(i -> AeronUtils.stringToByteBuffer(String.valueOf(i))).log());

    result.block();
  }

  static class FakeMessagePublication implements MessagePublication {

    private final Queue<Command> commands = new ConcurrentLinkedQueue<>();

    private final UnicastProcessor<ByteBuffer> messageProcessor;

    private final FluxSink<ByteBuffer> sink;

    FakeMessagePublication() {
      this.messageProcessor = UnicastProcessor.create();
      this.sink = messageProcessor.sink();
    }

    void publishSuccessfully(int nSignals) {
      commands.add(new SuccessfulPublicationCommand(nSignals));
    }

    void failPublication(long errorCode) {
      commands.add(new FailedPublicationCommand(errorCode));
    }

    @Override
    public long publish(MessageType msgType, ByteBuffer msgBody, long sessionId) {
      Command command = commands.peek();
      long result;
      if (command != null) {
        sink.next(msgBody);

        result = command.execute();
        if (command.isCompleted()) {
          commands.poll();
        }
      } else {
        throw new IllegalStateException("Command queue is empty");
      }
      return result;
    }

    public ByteBufferFlux messages() {
      return new ByteBufferFlux(messageProcessor);
    }

    @Override
    public String asString() {
      return "fakePublication";
    }

    interface Command {

      long execute();

      boolean isCompleted();
    }

    static class SuccessfulPublicationCommand implements Command {

      private int countdown;

      SuccessfulPublicationCommand(int countdown) {
        this.countdown = countdown;
      }

      @Override
      public long execute() {
        if (countdown > 0) {
          countdown--;
        } else {
          throw new IllegalStateException("Command has completed");
        }
        return 42;
      }

      @Override
      public boolean isCompleted() {
        return countdown == 0;
      }
    }

    static class FailedPublicationCommand implements Command {

      private final long errorCode;

      private volatile boolean completed;

      FailedPublicationCommand(long errorCode) {
        this.errorCode = errorCode;
      }

      @Override
      public long execute() {
        if (!completed) {
          completed = true;
          return errorCode;
        } else {
          throw new IllegalStateException("Command has completed");
        }
      }

      @Override
      public boolean isCompleted() {
        return completed;
      }
    }
  }
}
