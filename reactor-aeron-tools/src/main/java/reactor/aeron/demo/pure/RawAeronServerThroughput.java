package reactor.aeron.demo.pure;

import io.aeron.Aeron;
import io.aeron.Image;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.demo.RateReporter;

public class RawAeronServerThroughput {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronServerThroughput.class);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    Aeron aeron = RawAeronResources.start();
    new Server(aeron).start();
  }

  private static class Server extends RawAeronServer {

    private final RateReporter reporter = new RateReporter();

    Server(Aeron aeron) throws Exception {
      super(aeron);
    }

    @Override
    int processInbound(List<Image> images) {
      int result = 0;
      for (Image image : images) {
        try {
          result +=
              image.poll((buffer, offset, length, header) -> reporter.onMessage(1, length), 32);
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on inbound.poll(): ", ex);
        }
      }
      return result;
    }
  }
}
