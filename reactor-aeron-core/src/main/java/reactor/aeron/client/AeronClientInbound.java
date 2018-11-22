package reactor.aeron.client;

import io.aeron.Image;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronResources;
import reactor.aeron.ByteBufferFlux;
import reactor.aeron.DataMessageSubscriber;
import reactor.aeron.MessageType;
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

final class AeronClientInbound implements AeronInbound, Disposable {

  private static final Logger logger = Loggers.getLogger(AeronClientInbound.class);

  private final ByteBufferFlux flux;

  private final io.aeron.Subscription serverDataSubscription;

  private final AeronResources aeronResources;

  private final long sessionId;

  private final Runnable onComplete;

  AeronClientInbound(
      String name,
      AeronResources aeronResources,
      String channel,
      int streamId,
      long sessionId,
      Runnable onComplete) {
    this.aeronResources = aeronResources;
    this.onComplete = onComplete;
    ClientDataMessageProcessor processor =
        new ClientDataMessageProcessor(name, sessionId, onComplete);
    this.flux = new ByteBufferFlux(processor);
    this.sessionId = sessionId;
    this.serverDataSubscription =
        aeronResources.dataSubscription(
            name,
            channel,
            streamId,
            "to receive data from server on",
            sessionId,
            processor,
            null,
            this::onUnavailableDataImage);
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    aeronResources.close(serverDataSubscription);
  }

  private void onUnavailableDataImage(Image image) {
    if (image.subscription() == serverDataSubscription && serverDataSubscription.hasNoImages()) {
      onComplete.run();
    }
  }

  static class ClientDataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private final String category;

    private final long sessionId;

    private volatile Subscription subscription;

    private volatile Subscriber<? super ByteBuffer> subscriber;

    private final Runnable onCompleteHandler;

    ClientDataMessageProcessor(String category, long sessionId, Runnable onCompleteHandler) {
      this.category = category;
      this.sessionId = sessionId;
      this.onCompleteHandler = onCompleteHandler;
    }

    @Override
    public void onSubscribe(org.reactivestreams.Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
      if (sessionId != this.sessionId) {
        throw new RuntimeException(
            "Received " + MessageType.NEXT + " for unknown sessionId: " + sessionId);
      }
      subscriber.onNext(buffer);
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
