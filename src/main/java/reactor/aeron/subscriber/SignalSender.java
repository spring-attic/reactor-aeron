package reactor.aeron.subscriber;

import reactor.aeron.utils.SignalType;
import uk.co.real_logic.aeron.Publication;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public interface SignalSender {

	long publishSignal(String sessionId, Publication publication, ByteBuffer buffer, SignalType signalType,
					   boolean retryPublication);

}
