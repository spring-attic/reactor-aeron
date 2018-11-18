package reactor.ipc.aeron.server;

import io.aeron.Subscription;
import io.aeron.driver.AeronResources;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.DataMessageSubscriber;
import reactor.ipc.aeron.MessageType;
import reactor.util.Logger;
import reactor.util.Loggers;

final class AeronServerInbound implements AeronInbound, Disposable {

  private final TopicProcessor<ByteBuffer> processor;
  private final ByteBufferFlux flux;

  private final AeronResources aeronResources;

  private Subscription serverDataSubscription;
  private ServerDataMessageProcessor messageProcessor;

  AeronServerInbound(String name, AeronResources aeronResources) {
    this.processor = TopicProcessor.<ByteBuffer>builder().name(name).build();
    this.aeronResources = aeronResources;
    this.flux = new ByteBufferFlux(processor);
  }

  Mono<Void> initialise(
      String name,
      String channel,
      int serverSessionStreamId,
      long sessionId,
      Runnable onCompleteHandler) {
    messageProcessor = new ServerDataMessageProcessor(name, sessionId, onCompleteHandler);
    serverDataSubscription =
        aeronResources.dataSubscription(
            name,
            channel,
            serverSessionStreamId,
            "to receive client data on",
            sessionId,
            messageProcessor);
    messageProcessor.subscribe(processor);
    return Mono.empty(); // FIXME Mono.fromRunnable()
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    processor.onComplete();
    aeronResources.close(serverDataSubscription);
  }

  long lastSignalTimeNs() {
    return messageProcessor.lastSignalTimeNs;
  }

  static class ServerDataMessageProcessor implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private static final Logger logger = Loggers.getLogger(ServerDataMessageProcessor.class);

    private final String category;

    private volatile org.reactivestreams.Subscription subscription;

    private volatile long lastSignalTimeNs;

    private volatile Subscriber<? super ByteBuffer> subscriber;

    private final long sessionId;

    private final Runnable onCompleteHandler;

    ServerDataMessageProcessor(String category, long sessionId, Runnable onCompleteHandler) {
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
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}, buffer: {}",
            category,
            MessageType.NEXT,
            sessionId,
            buffer);
      }

      lastSignalTimeNs = System.nanoTime();

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
