package reactor.ipc.aeron;

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        System.setProperty(Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME, bufferLength);
        System.setProperty(Configuration.COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, bufferLength);
        System.setProperty(Configuration.PUBLICATION_LINGER_PROP_NAME,
                String.valueOf(TimeUnit.MILLISECONDS.toNanos(500)));
    }

    public static ByteBuffer stringToByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }

    public static Flux<ByteBuffer> newByteBufferFlux(String... items) {
        return Flux.just(items).map(AeronTestUtils::stringToByteBuffer);
    }

}
