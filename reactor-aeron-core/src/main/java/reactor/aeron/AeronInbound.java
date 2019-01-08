package reactor.aeron;

public interface AeronInbound {

  ByteBufferFlux receive();
}
