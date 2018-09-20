package reactor.ipc.aeron;

import io.aeron.driver.Configuration;
import java.util.concurrent.TimeUnit;

/** @author Anatoly Kadyshev */
class AeronTestUtils {

  static void setAeronEnvProps() {
    String bufferLength = String.valueOf(128 * 1024);

    System.setProperty(Configuration.TERM_BUFFER_LENGTH_PROP_NAME, bufferLength);
    System.setProperty(Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME, bufferLength);
    System.setProperty(Configuration.COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, bufferLength);
    System.setProperty(
        Configuration.PUBLICATION_LINGER_PROP_NAME,
        String.valueOf(TimeUnit.MILLISECONDS.toNanos(500)));
  }
}
