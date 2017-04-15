package reactor.ipc.aeron;

import groovy.json.internal.Charsets;
import reactor.core.publisher.Flux;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class AeronTestUtils {

    public static void setAeronEnvProps() {
        String bufferLength = String.valueOf(128 * 1024);
        System.setProperty(MediaDriver.DIRS_DELETE_ON_START_PROP_NAME, "true");

        System.setProperty(Configuration.TERM_BUFFER_LENGTH_PROP_NAME, bufferLength);
        System.setProperty(Configuration.TERM_BUFFER_MAX_LENGTH_PROP_NAME, bufferLength);
        System.setProperty(Configuration.COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, bufferLength);
        System.setProperty(Configuration.PUBLICATION_LINGER_PROP_NAME,
                String.valueOf(TimeUnit.MILLISECONDS.toNanos(500)));
    }

    public static ByteBuffer stringToByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes(Charsets.UTF_8));
    }

    public static Flux<ByteBuffer> newByteBufferFlux(String... items) {
        return Flux.just(items).map(AeronTestUtils::stringToByteBuffer);
    }

}
