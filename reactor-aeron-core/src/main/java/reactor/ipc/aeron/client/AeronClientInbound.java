package reactor.ipc.aeron.client;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronResources;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessageType;
import reactor.util.Logger;
import reactor.util.Loggers;

final class AeronClientInbound implements AeronInbound, Disposable {

  private static final Logger logger = Loggers.getLogger(AeronClientInbound.class);

  private final ByteBufferFlux flux;

  private final io.aeron.Subscription serverDataSubscription;

  private final ClientDataMessageProcessor processor;

  private final AeronResources aeronResources;

  AeronClientInbound(
      String name,
      AeronResources aeronResources,
      String channel,
      int streamId,
      long sessionId,
      Runnable onCompleteHandler) {
    this.aeronResources = aeronResources;
    this.processor = new ClientDataMessageProcessor(name, sessionId, onCompleteHandler);
    this.flux = new ByteBufferFlux(processor);
    this.serverDataSubscription =
        aeronResources.dataSubscription(
            channel, streamId, "to receive data from server on", sessionId, processor);
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    aeronResources.release(serverDataSubscription);
  }

  long getLastSignalTimeNs() {
    return processor.lastSignalTimeNs;
  }

  static class ClientDataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private final String category;

    private final long sessionId;

    private volatile long lastSignalTimeNs = 0;

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
      lastSignalTimeNs = System.nanoTime();
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
