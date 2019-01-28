package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.WorkerFlightRecorder;
import reactor.aeron.WorkerMBean;
import reactor.aeron.demo.RateReporter;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

  private static class Server {

    private static final String address = "localhost";
    private static final int port = 13000;
    private static final int controlPort = 13001;
    private static final int STREAM_ID = 0xcafe0000;
    private static final String acceptUri =
        new ChannelUriStringBuilder()
            .endpoint(address + ':' + port)
            .reliable(Boolean.TRUE)
            .media("udp")
            .build();
    private static final ChannelUriStringBuilder outboundChannelBuilder =
        new ChannelUriStringBuilder()
            .controlEndpoint(address + ':' + controlPort)
            .reliable(Boolean.TRUE)
            .media("udp");

    private final Aeron aeron;

    private volatile Subscription acceptSubscription;
    private final Map<Integer, Publication> publications = new ConcurrentHashMap<>();

    private final Scheduler scheduler = Schedulers.newSingle("server@" + this);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);
    private final RateReporter reporter = new RateReporter(Duration.ofSeconds(1));
    private final WorkerFlightRecorder flightRecorder;

    Server(Aeron aeron) throws Exception {
      this.aeron = aeron;

      this.flightRecorder = new WorkerFlightRecorder();
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName("reactor.aeron:name=" + "server@" + this);
      StandardMBean standardMBean = new StandardMBean(flightRecorder, WorkerMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
    }

    private void start() {
      scheduler.schedule(
          () -> {
            acceptSubscription =
                aeron.addSubscription(
                    acceptUri,
                    STREAM_ID,
                    this::onAcceptImageAvailable,
                    this::onAcceptImageUnavailable);

            scheduler.schedule(
                () -> {
                  flightRecorder.begin();

                  while (true) {
                    flightRecorder.countTick();

                    int i = processOutbound();
                    flightRecorder.countOutbound(i);

                    int j = processInbound();
                    flightRecorder.countInbound(j);

                    int workCount = i + j;
                    if (workCount < 1) {
                      flightRecorder.countIdle();
                    } else {
                      flightRecorder.countWork(workCount);
                    }

                    // Reporting
                    flightRecorder.tryReport();

                    idleStrategy.idle(workCount);
                  }
                });
          });
    }

    private void onAcceptImageAvailable(Image image) {
      int sessionId = image.sessionId();
      String outboundChannel = outboundChannelBuilder.sessionId(sessionId).build();

      logger.debug(
          "onImageAvailable: {} {}, create outbound {}",
          Integer.toHexString(image.sessionId()),
          image.sourceIdentity(),
          outboundChannel);

      scheduler.schedule(
          () -> {
            Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);
            publications.put(sessionId, publication);
          });
    }

    private void onAcceptImageUnavailable(Image image) {
      int sessionId = image.sessionId();

      logger.debug(
          "onImageUnavailable: {} {}", Integer.toHexString(sessionId), image.sourceIdentity());

      scheduler.schedule(
          () -> {
            Publication publication = publications.remove(sessionId);
            if (publication != null) {
              publication.close();
            }
          });
    }

    private int processInbound() {
      int result = 0;
      for (Image image : acceptSubscription.images()) {
        try {
          result +=
              image.poll(
                  (buffer, offset, length, header) -> reporter.onMessage(1, buffer.capacity()), 8);
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on inbound.poll(): ", ex);
        }
      }
      return result;
    }

    private int processOutbound() {
      return 0;
    }
  }
}
