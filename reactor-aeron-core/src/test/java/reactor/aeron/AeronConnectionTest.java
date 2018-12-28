package reactor.aeron;

import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.ChannelUriStringBuilder;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronOptions.Builder;
import reactor.aeron.client.AeronClient;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class AeronConnectionTest extends BaseAeronTest {

  private ChannelUriStringBuilder serverChannel;
  private ChannelUriStringBuilder clientChannel;
  private AeronResources aeronResources;
  private Duration imageLivenessTimeout;

  @BeforeEach
  void beforeEach() {
    serverChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:" + SocketUtils.findAvailableUdpPort(13000, 14000));
    clientChannel =
        new ChannelUriStringBuilder()
            .reliable(TRUE)
            .media("udp")
            .endpoint("localhost:" + SocketUtils.findAvailableUdpPort(14000, 15000));
    imageLivenessTimeout = Duration.ofSeconds(1);
    aeronResources =
        AeronResources.start(
            AeronResourcesConfig //
                .builder()
                .imageLivenessTimeout(imageLivenessTimeout)
                .build());
  }

  @AfterEach
  void afterEach() {
    if (aeronResources != null) {
      aeronResources.dispose();
      aeronResources.onDispose().block(TIMEOUT);
    }
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
            options -> options.clientChannel(clientChannel).serverChannel(serverChannel));

    CountDownLatch latch = new CountDownLatch(1);
    connection.onDispose().doOnSuccess(aVoid -> latch.countDown()).subscribe();

    connection.inbound().receiveAsString().log("client").subscribe(processor);

    processor.take(1).blockLast(Duration.ofSeconds(4));

    server.dispose();

    latch.await(imageLivenessTimeout.toMillis(), TimeUnit.MILLISECONDS);

    assertEquals(0, latch.getCount());
  }

  @Test
  public void testServerDisconnects() throws Exception {
    OnDisposable server = createServer(OnDisposable::onDispose);

    CountDownLatch clientConnectionLatch = new CountDownLatch(1);

    Connection client = createConnection();

    client.onDispose().doFinally(s -> clientConnectionLatch.countDown()).subscribe();

    Mono //
        .delay(Duration.ofSeconds(1))
        .doOnSuccess(avoid -> server.dispose())
        .subscribe();

    boolean await = clientConnectionLatch.await(3, TimeUnit.SECONDS);
    assertTrue(await, "clientConnectionLatch: " + clientConnectionLatch.getCount());
  }

  @Test
  public void testServerDisconnectsAndClientCleanups() throws Exception {
    OnDisposable server = createServer(OnDisposable::onDispose);

    CountDownLatch clientConnectionLatch = new CountDownLatch(2);

    Connection client = createConnection();

    client
        .inbound() //
        .receive()
        .log("CLIENT_INBOUND")
        .doFinally(s -> clientConnectionLatch.countDown())
        .then()
        .subscribe();
    client
        .outbound()
        .send(
            Mono.<ByteBuffer>never()
                .log("CLIENT_OUTBOUND_SEND")
                .doFinally(s -> clientConnectionLatch.countDown()))
        .then()
        .log("CLIENT_OUTBOUND")
        .subscribe();

    Mono //
        .delay(Duration.ofSeconds(1))
        .doOnSuccess(avoid -> server.dispose())
        .subscribe();

    boolean await = clientConnectionLatch.await(3, TimeUnit.SECONDS);
    assertTrue(await, "clientConnectionLatch: " + clientConnectionLatch.getCount());
  }

  @Test
  public void testClientDisconnects() throws Exception {
    CountDownLatch serverConnectionLatch = new CountDownLatch(1);

    createServer(c -> c.onDispose().doFinally(s -> serverConnectionLatch.countDown()));

    Connection client = createConnection();

    Mono //
        .delay(Duration.ofSeconds(1))
        .doOnSuccess(avoid -> client.dispose())
        .subscribe();

    boolean await = serverConnectionLatch.await(3, TimeUnit.SECONDS);
    assertTrue(await, "serverConnectionLatch: " + serverConnectionLatch.getCount());
  }

  @Test
  public void testClientDisconnectsAndServerCleanups() throws Exception {
    CountDownLatch serverConnectionLatch = new CountDownLatch(2);

    createServer(
        c -> {
          c.inbound() //
              .receive()
              .log("SERVER_INBOUND")
              .doFinally(s -> serverConnectionLatch.countDown())
              .then()
              .subscribe();
          c.outbound()
              .send(
                  Mono.<ByteBuffer>never()
                      .log("SERVER_OUTBOUND_SEND")
                      .doFinally(s -> serverConnectionLatch.countDown()))
              .then()
              .log("SERVER_OUTBOUND")
              .subscribe();
          return c.onDispose();
        });

    Connection client = createConnection();

    Mono //
        .delay(Duration.ofSeconds(1))
        .doOnSuccess(avoid -> client.dispose())
        .subscribe();

    boolean await = serverConnectionLatch.await(3, TimeUnit.SECONDS);
    assertTrue(await, "serverConnectionLatch: " + serverConnectionLatch.getCount());
  }

  private Connection createConnection(Consumer<Builder> options) {
    return AeronClient.create(aeronResources).options(options).connect().block(TIMEOUT);
  }

  private OnDisposable createServer(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return AeronServer.create(aeronResources)
        .options(options -> options.serverChannel(serverChannel))
        .handle(handler)
        .bind()
        .block(TIMEOUT);
  }

  private Connection createConnection() {
    return createConnection(
        options -> options.clientChannel(clientChannel).serverChannel(serverChannel));
  }
}
