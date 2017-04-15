package reactor.ipc.aeron;

import reactor.ipc.aeron.server.AeronFlux;
import reactor.ipc.connector.Inbound;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public interface AeronInbound extends Inbound<ByteBuffer> {

    @Override
    AeronFlux receive();

}
