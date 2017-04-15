package reactor.ipc.aeron;

import reactor.ipc.connector.Connector;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public interface AeronConnector extends Connector<ByteBuffer, ByteBuffer, AeronInbound, AeronOutbound> {

}
