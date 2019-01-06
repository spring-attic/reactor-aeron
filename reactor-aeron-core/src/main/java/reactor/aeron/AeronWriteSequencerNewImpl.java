package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

final class AeronWriteSequencerNewImpl {

  private final MessagePublication publication;

  /**
   * Constructor for templating {@link AeronWriteSequencerNewImpl} objects.
   *
   * @param publication message publication
   */
  AeronWriteSequencerNewImpl(MessagePublication publication) {
    this.publication = Objects.requireNonNull(publication, "message publication must be present");
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencerNewImpl} instance.
   *
   * @param publisher data publisher
   * @return mono handle
   */
  public Mono<Void> write(Publisher<? extends ByteBuffer> publisher) {
    Objects.requireNonNull(publisher, "publisher must be not null");
    return Mono.defer(
        () -> {
          if (publication.isDisposed()) {
            return Mono.error(AeronExceptions.failWithMessagePublicationUnavailable());
          }
          PublisherSender inner = new PublisherSender(publication);
          publisher.subscribe(inner);
          return inner
              .promise()
              .doOnCancel(inner::hookOnCancel)
              .takeUntilOther(
                  publication
                      .onDispose()
                      .then(
                          Mono.defer(
                              () ->
                                  Mono.error(
                                      AeronExceptions.failWithMessagePublicationUnavailable()))))
              .doOnCancel(inner::cancel);
        });
  }

  private static class PublisherSender extends BaseSubscriber<ByteBuffer> {

    private static AtomicInteger count = new AtomicInteger();

    private static final int PREFETCH = 32;

    private final MessagePublication publication;

    private final MonoProcessor<Void> onCompleted = MonoProcessor.create();

    private final MonoProcessor<Void> onAllSent = MonoProcessor.create();

    private final Map<Integer, Subscription> disposables = new ConcurrentHashMap<>(PREFETCH, 1);

    Mono<Void> promise() {
      return Mono.whenDelayError(onCompleted, onAllSent);
    }

    private PublisherSender(MessagePublication publication) {
      this.publication = publication;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      subscription.request(PREFETCH);
    }

    @Override
    protected void hookOnNext(ByteBuffer value) {
      int id = count.incrementAndGet();

      publication
          .enqueue(value)
          .doOnSubscribe(
              subscription -> {
                disposables.put(id, subscription);
              })
          .doOnSuccess(
              avoid -> {
                disposables.remove(id);
                if (!onCompleted.isDisposed()) {
                  request(1L);
                }

                if (onCompleted.isDisposed() && disposables.isEmpty() && !onAllSent.isDisposed()) {
                  onAllSent.onComplete();
                }
              })
          .doOnCancel(
              () -> {
                if (onCompleted.isDisposed() && disposables.isEmpty() && !onAllSent.isDisposed()) {
                  onAllSent.onComplete();
                }
              })
          .subscribe(
              null,
              th -> {
                disposables.remove(id);
                if (!onCompleted.isDisposed()) {
                  onCompleted.onError(new Exception("Failed to publish signal", th));
                }
                hookOnCancel();
              });
    }

    @Override
    protected void hookOnComplete() {
      onCompleted.onComplete();
      if (disposables.isEmpty()) {
        onAllSent.onComplete();
      }
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      onCompleted.onError(throwable);

      // todo do we need to cancel pending tasks or wait for their will be finished?
      disposables
          .values()
          .removeIf(
              subscription -> {
                subscription.cancel();
                return true;
              });

      if (disposables.isEmpty()) {
        onAllSent.onComplete();
      }
    }

    @Override
    protected void hookOnCancel() {
      disposables
          .values()
          .removeIf(
              subscription -> {
                subscription.cancel();
                return true;
              });
      if (!onCompleted.isDisposed()) {
        onCompleted.onComplete();
      }
      if (!onAllSent.isDisposed()) {
        onAllSent.onComplete();
      }
    }
  }
}
