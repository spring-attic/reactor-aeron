package reactor.aeron;

import java.util.UUID;
import org.reactivestreams.Subscription;

public interface ControlMessageSubscriber {

  void onSubscription(Subscription subscription);

  void onConnect(
      UUID connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId);

  void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId);

  void onComplete(long sessionId);
}
