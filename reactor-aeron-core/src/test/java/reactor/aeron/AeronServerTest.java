package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.aeron.client.AeronClient;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class AeronServerTest extends BaseAeronTest {

  private int serverPort;
  private int serverControlPort;
  private AeronResources aeronResources;

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    aeronResources = AeronResources.start(AeronResourcesConfig.builder().build());
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
        .send(ByteBufferFlux.fromString("Hello", "world!").log("send"))
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
        .sendString(
            Flux.range(1, 100)
                .delayElements(Duration.ofSeconds(1))
                .map(String::valueOf)
                .log("send"))
        .then()
        .subscribe();

    processor.blockFirst();

    server.dispose();

    ThreadWatcher threadWatcher = new ThreadWatcher();

    assertTrue(threadWatcher.awaitTerminated(5000, "single-", "parallel-"));
  }

  private Connection createConnection() {
    return AeronClient.create(aeronResources)
        .options("localhost", serverPort, serverControlPort)
        .connect()
        .block(TIMEOUT);
  }

  private OnDisposable createServer(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return AeronServer.create(aeronResources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
        .bind()
        .block(TIMEOUT);
  }
}
