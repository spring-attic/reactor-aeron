package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.demo.raw.RawAeronResources.MsgPublication;

public class RawAeronClientThroughput {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronClientThroughput.class);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    Aeron aeron = RawAeronResources.start();
    new Client(aeron).start();
  }

  private static class Client extends RawAeronClient {

    private final DirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    Client(Aeron aeron) throws Exception {
      super(aeron);
    }

    @Override
    int processOutbound(MsgPublication msgPublication) {
      return msgPublication.proceed(buffer);
    }
  }
}
