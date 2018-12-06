package reactor.aeron.server;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronResources;
import reactor.aeron.ByteBufferFlux;
import reactor.aeron.DataMessageSubscriber;
import reactor.aeron.InnerPoller;
import reactor.aeron.MessageType;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;

final class AeronServerInbound implements AeronInbound, OnDisposable {

  private final String name;
  private final AeronResources resources;
  private final TopicProcessor<ByteBuffer> processor;
  private final ByteBufferFlux flux;

  private volatile InnerPoller subscription;

  AeronServerInbound(String name, AeronResources resources) {
    this.name = name;
    this.resources = resources;
    this.processor = TopicProcessor.<ByteBuffer>builder().name(name).build();
    this.flux = new ByteBufferFlux(processor);
  }

  Mono<Void> start(String channel, int streamId, long sessionId, Runnable onCompleteHandler) {
    return Mono.defer(
        () -> {
          ServerDataMessageProcessor messageProcessor =
              new ServerDataMessageProcessor(name, sessionId, onCompleteHandler);

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
                    messageProcessor.accept(subscription);
                    messageProcessor.subscribe(processor);
                  })
              .then()
              .log("serverInbound");
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
    processor.onComplete();
  }

  @Override
  public Mono<Void> onDispose() {
    return subscription != null ? subscription.onDispose() : Mono.empty();
  }

  @Override
  public boolean isDisposed() {
    return subscription != null && subscription.isDisposed();
  }

  private static class ServerDataMessageProcessor
      implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private static final Logger logger = LoggerFactory.getLogger(ServerDataMessageProcessor.class);

    private final String category;
    private final long sessionId;
    private final Runnable onCompleteHandler;

    private volatile org.reactivestreams.Subscription subscription;
    private volatile Subscriber<? super ByteBuffer> subscriber;

    private ServerDataMessageProcessor(
        String category, long sessionId, Runnable onCompleteHandler) {
      this.category = category;
      this.sessionId = sessionId;
      this.onCompleteHandler = onCompleteHandler;
    }

    @Override
    public void accept(org.reactivestreams.Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}, buffer: {}",
            category,
            MessageType.NEXT,
            sessionId,
            buffer);
      }

      if (this.sessionId == sessionId) {
        subscriber.onNext(buffer);
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}", category, MessageType.NEXT, sessionId);
      }
    }

    @Override
    public void onComplete(long sessionId) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
      }

      if (this.sessionId == sessionId) {
        onCompleteHandler.run();
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}",
            category,
            MessageType.COMPLETE,
            sessionId);
      }
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(subscription);
    }
  }
}
