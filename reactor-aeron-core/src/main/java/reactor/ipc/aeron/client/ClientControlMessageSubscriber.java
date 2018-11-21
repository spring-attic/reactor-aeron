package reactor.ipc.aeron.client;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.client.AeronClientInbound.ClientDataMessageProcessor;
import reactor.util.Logger;
import reactor.util.Loggers;

class ClientControlMessageSubscriber implements ControlMessageSubscriber {

  private final Logger logger = Loggers.getLogger(ClientControlMessageSubscriber.class);

  private final String category;


  private final Consumer<Long> onCompleteHandler;

  private final Map<UUID, MonoProcessor<ConnectAckResponse>> sinkByConnectRequestId =
      new ConcurrentHashMap<>();

  ClientControlMessageSubscriber(
      String category, Consumer<Long> onCompleteHandler) {
    this.category = category;
    this.onCompleteHandler = onCompleteHandler;
  }

  @Override
  public void onSubscribe(org.reactivestreams.Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.debug(
        "[{}] Received {} for connectRequestId: {}, serverSessionStreamId: {}",
        category,
        MessageType.CONNECT_ACK,
        connectRequestId,
        serverSessionStreamId);

    MonoProcessor<ConnectAckResponse> processor = sinkByConnectRequestId.remove(connectRequestId);
    if (processor != null) {
      processor.onNext(new ConnectAckResponse(sessionId, serverSessionStreamId));
      processor.onComplete();
    }
  }

  /**
   * Handler for complete signal from server. At the moment of writing this javadoc the server
   * doesn't emit complete signal. Method is left with logging.
   *
   * <p>See for details: {@link MessageType#COMPLETE}, {@link Protocol#createDisconnectBody(long)},
   * {@link ClientDataMessageProcessor#onComplete(long)}.
   *
   * @param sessionId session id
   */
  @Override
  public void onComplete(long sessionId) {
    logger.info("[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
    onCompleteHandler.accept(sessionId);
  }

  @Override
  public void onConnect(
      UUID connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {
    logger.error(
        "[{}] Unsupported {} request for a client, clientChannel: {}, "
            + "clientControlStreamId: {}, clientSessionStreamId: {}",
        category,
        MessageType.CONNECT,
        clientChannel,
        clientControlStreamId,
        clientSessionStreamId);
  }

  ConnectAckSubscription subscribeForConnectAck(UUID connectRequestId) {
    MonoProcessor<ConnectAckResponse> processor = MonoProcessor.create();
    sinkByConnectRequestId.put(connectRequestId, processor);
    return new ConnectAckSubscription(processor, connectRequestId);
  }

  class ConnectAckSubscription implements Disposable {

    private final MonoProcessor<ConnectAckResponse> processor;

    private final UUID connectRequestId;

    ConnectAckSubscription(MonoProcessor<ConnectAckResponse> processor, UUID connectRequestId) {
      this.processor = processor;
      this.connectRequestId = connectRequestId;
    }

    Mono<ConnectAckResponse> connectAck() {
      return processor;
    }

    @Override
    public void dispose() {
      sinkByConnectRequestId.remove(connectRequestId);
      processor.cancel();
    }
  }

  static class ConnectAckResponse {

    final long sessionId;

    final int serverSessionStreamId;

    ConnectAckResponse(long sessionId, int serverSessionStreamId) {
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
    }
  }
}
