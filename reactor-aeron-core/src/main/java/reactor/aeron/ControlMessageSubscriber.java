package reactor.aeron;

import java.util.UUID;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;

public interface ControlMessageSubscriber extends Consumer<Subscription> {

  void onConnect(
      UUID connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId);

  void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId);

  void onComplete(long sessionId);
}
