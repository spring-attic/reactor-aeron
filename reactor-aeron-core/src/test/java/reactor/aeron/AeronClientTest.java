package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.aeron.client.AeronClient;
import reactor.aeron.client.AeronClientOptions;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

public class AeronClientTest extends BaseAeronTest {

  private static String serverChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort(13000);

  private static String clientChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();

  private static int imageLivenessTimeoutSec = 1;

  private static AeronResources aeronResources;

  private static final Consumer<AeronClientOptions> DEFAULT_CLIENT_OPTIONS =
      options -> {
        options.clientChannel(clientChannel);
        options.serverChannel(serverChannel);
      };

  @BeforeAll
  static void beforeAll() {
    aeronResources =
        AeronResources.start(
            AeronResourcesConfig.builder()
                .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(imageLivenessTimeoutSec))
                .build());
  }

  @AfterAll
  static void afterAll() {
    Optional.ofNullable(aeronResources).ifPresent(AeronResources::dispose);
  }

  @Test
  public void testClientCouldNotConnectToServer() {
    assertThrows(RuntimeException.class, this::createConnection);
  }

  @Test
  public void testClientReceivesDataFromServer() {
    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.from("hello1", "2", "3").log("server"))
                .then(connection.onDispose()));

    Connection connection = createConnection();
    StepVerifier.create(connection.inbound().receive().asString().log("client"))
        .expectNext("hello1", "2", "3")
        .expectNoEvent(Duration.ofMillis(10))
        .thenCancel()
        .verify();
  }

  @Test
  public void testTwoClientsReceiveDataFromServer() {
    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.from("1", "2", "3").log("server"))
                .then(connection.onDispose()));

    Connection connection1 = createConnection();
    Connection connection2 = createConnection();

    StepVerifier.create(connection1.inbound().receive().asString().log("client-1"))
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();

    StepVerifier.create(connection2.inbound().receive().asString().log("client-2"))
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testClientWith2HandlersReceiveData() {
    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.from("1", "2", "3").log("server"))
                .then(connection.onDispose()));

    ReplayProcessor<String> processor1 = ReplayProcessor.create();
    ReplayProcessor<String> processor2 = ReplayProcessor.create();

    addDisposable(
        AeronClient.create(aeronResources)
            .options(DEFAULT_CLIENT_OPTIONS)
            .handle(
                connection -> {
                  connection.inbound().receive().asString().log("client-1").subscribe(processor1);
                  return connection.onDispose();
                })
            .connect()
            .block(TIMEOUT));

    addDisposable(
        AeronClient.create(aeronResources)
            .options(DEFAULT_CLIENT_OPTIONS)
            .handle(
                connection -> {
                  connection.inbound().receive().asString().log("client-2").subscribe(processor2);
                  return connection.onDispose();
                })
            .connect()
            .block(TIMEOUT));

    StepVerifier.create(processor1)
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();

    StepVerifier.create(processor2)
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testClientClosesSessionAndServerHandleUnavailableImage() throws Exception {
    OnDisposable server =
        AeronServer.create(aeronResources)
            .options(
                options -> {
                  options.serverChannel(serverChannel);
                })
            .handle(
                connection ->
                    connection
                        .outbound()
                        .send(
                            ByteBufferFlux.from("hello1", "2", "3")
                                .delayElements(Duration.ofSeconds(1))
                                .log("server"))
                        .then(connection.onDispose()))
            .bind()
            .block(TIMEOUT);

    ReplayProcessor<String> processor = ReplayProcessor.create();

    Connection connection =
        createConnection(
            options -> {
              options.clientChannel(clientChannel);
              options.serverChannel(serverChannel);
            });

    CountDownLatch latch = new CountDownLatch(1);
    connection.onDispose().doOnSuccess(aVoid -> latch.countDown()).subscribe();

    connection.inbound().receive().asString().log("client").subscribe(processor);

    processor.take(1).blockLast(Duration.ofSeconds(4));

    server.dispose();

    latch.await(imageLivenessTimeoutSec, TimeUnit.SECONDS);

    assertEquals(0, latch.getCount());
  }

  private Connection createConnection() {
    return createConnection(DEFAULT_CLIENT_OPTIONS);
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
