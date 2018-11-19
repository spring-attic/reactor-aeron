package reactor.ipc.aeron;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.driver.AeronResources;
import java.time.Duration;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.client.AeronClientOptions;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

public class AeronClientTest extends BaseAeronTest {

  private static String serverChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort(13000);

  private static String clientChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();

  private static AeronResources aeronResources;

  private static final Consumer<AeronClientOptions> DEFAULT_CLIENT_OPTIONS =
      options -> {
        options.clientChannel(clientChannel);
        options.serverChannel(serverChannel);
      };

  @BeforeAll
  static void beforeAll() {
    aeronResources = new AeronResources("test");
  }

  @AfterAll
  static void afterAll() {
    if (aeronResources != null) {
      aeronResources.dispose();
    }
  }

  @Test
  public void testClientCouldNotConnectToServer() {
    assertThrows(RuntimeException.class, this::createConnection);
  }

  @Test
  public void testClientReceivesDataFromServer() {
    AeronServer server = createAeronServer("server");

    server
        .newHandler(
            (inbound, outbound) ->
                Mono.from(outbound.send(ByteBufferFlux.from("hello1", "2", "3").log("server"))))
        .block(TIMEOUT);

    Connection connection = createConnection();
    StepVerifier.create(connection.inbound().receive().asString().log("client"))
        .expectNext("hello1", "2", "3")
        .expectNoEvent(Duration.ofMillis(10))
        .thenCancel()
        .verify();
  }

  @Test
  public void testTwoClientsReceiveDataFromServer() throws InterruptedException {
    AeronServer server = createAeronServer("server");
    blockAndAddDisposable(
        server.newHandler(
            (inbound, outbound) -> {
              Mono.from(outbound.send(ByteBufferFlux.from("1", "2", "3").log())).subscribe();
              return Mono.never();
            }));

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
    AeronServer server = createAeronServer("server-1");

    blockAndAddDisposable(
        server.newHandler(
            (inbound, outbound) -> {
              Mono.from(outbound.send(ByteBufferFlux.from("1", "2", "3").log("server")))
                  .subscribe();
              return Mono.never();
            }));

    ReplayProcessor<String> processor1 = ReplayProcessor.create();
    ReplayProcessor<String> processor2 = ReplayProcessor.create();

    addDisposable(
        AeronClient.create(aeronResources)
            .options(DEFAULT_CLIENT_OPTIONS)
            .handle(
                (inbound, outbound) -> {
                  inbound.receive().asString().log("client-1").subscribe(processor1);
                  return Mono.never();
                })
            .connect()
            .block(TIMEOUT));

    addDisposable(
        AeronClient.create(aeronResources)
            .options(DEFAULT_CLIENT_OPTIONS)
            .handle(
                (inbound, outbound) -> {
                  inbound.receive().asString().log("client-2").subscribe(processor2);
                  return Mono.never();
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
  public void testClientClosesSessionUponHeartbeatLoss() throws Exception {
    AeronServer server =
        AeronServer.create(
            "server",
            aeronResources,
            options -> {
              options.serverChannel(serverChannel);
              options.heartbeatTimeoutMillis(500);
            });

    Disposable serverHandlerDisposable =
        server
            .newHandler(
                (inbound, outbound) ->
                    Mono.from(
                        outbound.send(
                            ByteBufferFlux.from("hello1", "2", "3")
                                .delayElements(Duration.ofSeconds(1))
                                .log("server"))))
            .block(TIMEOUT);

    ReplayProcessor<String> processor = ReplayProcessor.create();

    Connection connection =
        createConnection(
            options -> {
              options.clientChannel(clientChannel);
              options.serverChannel(serverChannel);
              options.heartbeatTimeoutMillis(500);
            });

    connection.inbound().receive().asString().log("client").subscribe(processor);

    processor.elementAt(2).block(Duration.ofSeconds(4));

    serverHandlerDisposable.dispose();

    Thread.sleep(1500);
  }

  private AeronServer createAeronServer(String name) {
    return AeronServer.create(
        name, aeronResources, options -> options.serverChannel(serverChannel));
  }

  private Connection createConnection() {
    return createConnection(DEFAULT_CLIENT_OPTIONS);
  }

  private Connection createConnection(Consumer<AeronClientOptions> options) {
    Connection connection =
        AeronClient.create(aeronResources).options(options).connect().block(TIMEOUT);
    return addDisposable(connection);
  }
}
