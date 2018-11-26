package reactor.aeron;

import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.ChannelUriStringBuilder;
import java.time.Duration;
import java.util.Arrays;
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
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class AeronClientTest extends BaseAeronTest {

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
  public void testClientReceivesLongDataFromServer() {
    aeronResources.mtuLength();
    char[] chars = new char[aeronResources.mtuLength() * 2];
    Arrays.fill(chars, 'a');
    String str = new String(chars);

    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.from(str, str, str).log("server"))
                .then(connection.onDispose()));

    Connection connection = createConnection();

    StepVerifier.create(connection.inbound().receive().asString().log("client"))
        .expectNext(str, str, str)
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

    createConnection(
        connection -> {
          connection.inbound().receive().asString().log("client-1").subscribe(processor1);
          return connection.onDispose();
        });

    createConnection(
        connection -> {
          connection.inbound().receive().asString().log("client-2").subscribe(processor2);
          return connection.onDispose();
        });

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
        createServer(
            connection ->
                connection
                    .outbound()
                    .send(
                        ByteBufferFlux.from("hello1", "2", "3")
                            .delayElements(Duration.ofSeconds(1))
                            .log("server1"))
                    .then(connection.onDispose()));

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

  private Connection createConnection(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    Connection connection =
        AeronClient.create(aeronResources)
            .options(
                options -> {
                  options.clientChannel(clientChannel);
                  options.serverChannel(serverChannel);
                })
            .handle(handler)
            .connect()
            .block(TIMEOUT);
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
