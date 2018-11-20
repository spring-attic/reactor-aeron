package reactor.ipc.aeron;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.AeronResources;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

public class AeronServerTest extends BaseAeronTest {

  private String serverChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort(13000);

  private String clientChannel =
      "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();

  private static AeronResources aeronResources;

  @BeforeAll
  static void beforeAll() {
    aeronResources = AeronResources.start();
  }

  @AfterAll
  static void afterAll() {
    Optional.ofNullable(aeronResources).ifPresent(AeronResources::dispose);
  }

  @Test
  public void testServerReceivesData() throws InterruptedException {
    AeronServer server = AeronServer.create(aeronResources);
    ReplayProcessor<String> processor = ReplayProcessor.create();
    Disposable serverHandlerDisposable =
        server
            .newHandler(
                (inbound, outbound) -> {
                  inbound.receive().asString().log("receive").subscribe(processor);
                  return Mono.never();
                })
            .block(TIMEOUT);

    AeronClient client = AeronClient.create(aeronResources);
    try {
      client
          .newHandler(
              (inbound, outbound) -> {
                outbound
                    .send(ByteBufferFlux.from("Hello", "world!").log("send"))
                    .then()
                    .subscribe();
                return Mono.never();
              })
          .block(TIMEOUT);

      StepVerifier.create(processor).expectNext("Hello", "world!").thenCancel().verify();
    } finally {
      serverHandlerDisposable.dispose();
      client.dispose();
    }
  }

  @Test
  public void testServerDisconnectsClientsUponShutdown() throws InterruptedException {
    AeronServer server = createAeronServer("server");
    ReplayProcessor<ByteBuffer> processor = ReplayProcessor.create();
    Disposable serverDisposable =
        server
            .newHandler(
                (inbound, outbound) -> {
                  inbound.receive().subscribe(processor);
                  return Mono.never();
                })
            .block();

    AeronClient client = createAeronClient("client");
    client
        .newHandler(
            (inbound, outbound) ->
                outbound.send(
                    Flux.range(1, 100)
                        .delayElements(Duration.ofSeconds(1))
                        .map(i -> AeronUtils.stringToByteBuffer("" + i))))
        .block();

    processor.blockFirst();

    serverDisposable.dispose();
    client.dispose();

    ThreadWatcher threadWatcher = new ThreadWatcher();

    assertTrue(threadWatcher.awaitTerminated(5000, "single-", "parallel-"));
  }

  @Test
  public void testServerDisconnectsSessionUponHeartbeatLoss() throws InterruptedException {
    AeronServer server = createAeronServer("server");
    ReplayProcessor<ByteBuffer> processor = ReplayProcessor.create();
    Disposable serverDisposable =
        server
            .newHandler(
                (inbound, outbound) -> {
                  inbound.receive().subscribe(processor);
                  return Mono.never();
                })
            .block();

    AeronClient client = createAeronClient("client");
    client
        .newHandler(
            (inbound, outbound) ->
                outbound.send(
                    Flux.range(1, 100)
                        .delayElements(Duration.ofSeconds(1))
                        .map(i -> AeronUtils.stringToByteBuffer("" + i))))
        .block();

    processor.blockFirst();

    client.dispose();
  }

  private AeronClient createAeronClient(String name) {
    return AeronClient.create(
        name,
        aeronResources,
        options -> {
          options.clientChannel(clientChannel);
          options.serverChannel(serverChannel);
        });
  }

  private AeronServer createAeronServer(String name) {
    return AeronServer.create(
        name, aeronResources, options -> options.serverChannel(serverChannel));
  }
}
