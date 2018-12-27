package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

public final class DefaultAeronInbound implements AeronInbound, OnDisposable {

  private final String name;
  private final AeronResources resources;

  private volatile ByteBufferFlux flux;
  private volatile MessageSubscription subscription;

  public DefaultAeronInbound(String name, AeronResources resources) {
    this.name = name;
    this.resources = resources;
  }

  /**
   * Starts inbound.
   *
   * @param channel server or client channel uri
   * @param streamId stream id
   * @param onCompleteHandler callback which will be invoked when this finishes
   * @return success result
   */
  public Mono<Void> start(String channel, int streamId, Runnable onCompleteHandler) {
    return Mono.defer(
        () -> {
          DataMessageProcessor messageProcessor = new DataMessageProcessor();

          flux = new ByteBufferFlux(messageProcessor);

          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .dataSubscription(
                  name,
                  channel,
                  streamId,
                  messageProcessor,
                  eventLoop,
                  null,
                  image -> Optional.ofNullable(onCompleteHandler).ifPresent(Runnable::run))
              .doOnSuccess(
                  result -> {
                    subscription = result;
                    messageProcessor.onSubscription(subscription);
                  })
              .then();
        });
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    if (subscription != null) {
      subscription.dispose();
    }
  }

  @Override
  public Mono<Void> onDispose() {
    return subscription != null ? subscription.onDispose() : Mono.empty();
  }

  @Override
  public boolean isDisposed() {
    return subscription != null && subscription.isDisposed();
  }

  private static class DataMessageProcessor
      implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private volatile Subscription subscription;
    private volatile Subscriber<? super ByteBuffer> subscriber;

    private DataMessageProcessor() {}

    @Override
    public void onSubscription(Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(ByteBuffer buffer) {
      subscriber.onNext(buffer);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(subscription);
    }
  }
}
