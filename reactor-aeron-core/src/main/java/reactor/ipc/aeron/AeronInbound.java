package reactor.ipc.aeron;

public interface AeronInbound {

  ByteBufferFlux receive();
}
