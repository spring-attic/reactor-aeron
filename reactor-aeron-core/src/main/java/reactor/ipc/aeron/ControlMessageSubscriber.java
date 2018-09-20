package reactor.ipc.aeron;

import java.util.UUID;

/** @author Anatoly Kadyshev */
public interface ControlMessageSubscriber extends PoolerSubscriber {

  void onConnect(
      UUID connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId);

  void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId);

  void onHeartbeat(long sessionId);
}
