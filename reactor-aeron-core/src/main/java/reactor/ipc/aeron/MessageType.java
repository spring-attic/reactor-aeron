package reactor.ipc.aeron;

public enum MessageType {
  CONNECT,
  CONNECT_ACK,
  HEARTBEAT,
  NEXT,
  COMPLETE
}
