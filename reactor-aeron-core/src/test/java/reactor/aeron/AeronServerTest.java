package reactor.aeron;

import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.ChannelUriStringBuilder;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.aeron.client.AeronClient;
import reactor.aeron.client.AeronClientOptions;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class AeronServerTest extends BaseAeronTest {

  private String serverChannel;
  private String clientChannel;
  private AeronResources aeronResources;

  private final Duration imageLivenessTimeout = Duration.ofSeconds(1);

  @BeforeEach
  void beforeEach() {
    serverChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:" + SocketUtils.findAvailableUdpPort(13000, 14000))
            .build();
    clientChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:" + SocketUtils.findAvailableUdpPort(14000, 15000))
            .build();
    aeronResources =
        AeronResources.start(
            AeronResourcesConfig.builder()
                .imageLivenessTimeoutNs(imageLivenessTimeout)
                .build());
  }

  @AfterEach
  void afterEach() {
    Optional.ofNullable(aeronResources).ifPresent(AeronResources::dispose);
  }

  @Test
  public void testServerReceivesData() {
    ReplayProcessor<String> processor = ReplayProcessor.create();

    createServer(
        connection -> {
          connection.inbound().receive().asString().log("receive").subscribe(processor);
          return connection.onDispose();
        });

    createConnection()
        .outbound()
        .send(ByteBufferFlux.from("Hello", "world!").log("send"))
        .then()
        .subscribe();

    StepVerifier.create(processor).expectNext("Hello", "world!").thenCancel().verify();
  }

  @Test
  public void testServerDisconnectsClientsUponShutdown() throws InterruptedException {
    ReplayProcessor<ByteBuffer> processor = ReplayProcessor.create();

    OnDisposable server =
        createServer(
            connection -> {
              connection.inbound().receive().log("receive").subscribe(processor);
              return connection.onDispose();
            });

    createConnection()
        .outbound()
        .send(
            Flux.range(1, 100)
                .delayElements(Duration.ofSeconds(1))
                .map(i -> AeronUtils.stringToByteBuffer("" + i))
                .log("send"))
        .then()
        .subscribe();

    processor.blockFirst();

    server.dispose();

    ThreadWatcher threadWatcher = new ThreadWatcher();

    assertTrue(threadWatcher.awaitTerminated(5000, "single-", "parallel-"));
  }

  @Test
  public void testServerDisconnectsSessionAndClientHandleUnavailableImage()
      throws InterruptedException {
    ReplayProcessor<ByteBuffer> processor = ReplayProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);

    createServer(
        connection -> {
          connection.onDispose().doOnSuccess(aVoid -> latch.countDown()).subscribe();
          connection.inbound().receive().subscribe(processor);
          return connection.onDispose();
        });

    Connection connection = createConnection();
    connection
        .outbound()
        .send(
            Flux.range(1, 100)
                .delayElements(Duration.ofSeconds(1))
                .map(i -> AeronUtils.stringToByteBuffer("" + i))
                .log("send"))
        .then()
        .subscribe();

    processor.blockFirst();

    connection.dispose();

    latch.await(imageLivenessTimeout.toMillis(), TimeUnit.MILLISECONDS);

    assertEquals(0, latch.getCount());
  }

  private Connection createConnection() {
    return createConnection(
        options -> {
          options.clientChannel(clientChannel);
          options.serverChannel(serverChannel);
        });
  }

  private Connection createConnection(Consumer<AeronClientOptions> options) {
    Connection connection =
        AeronClient.create(aeronResources).options(options).connect().block(TIMEOUT);
    return addDisposable(connection);
  }

  private OnDisposable createServer(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return addDisposable(
        AeronServer.create(aeronResources)
            .options(options -> options.serverChannel(serverChannel))
            .handle(handler)
            .bind()
            .block(TIMEOUT));
  }
}
