package reactor.aeron;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class AeronMultiClientTest extends BaseAeronTest {

  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);

  private int serverPort;
  private int serverControlPort;
  private AeronResources serverResources;
  private List<AeronResources> clientResources = new ArrayList<>();

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    serverResources = new AeronResources().useTmpDir().singleWorker().start().block();
  }

  @AfterEach
  void afterEach() {
    Mono.whenDelayError(
            clientResources
                .stream()
                .peek(AeronResources::dispose)
                .map(AeronResources::onDispose)
                .collect(Collectors.toList()))
        .block(TIMEOUT);
    if (serverResources != null) {
      serverResources.dispose();
      serverResources.onDispose().block(TIMEOUT);
    }
  }

  @Test
  public void testMultiClientWithTheSameSessionId() {
    int sessionId = 100500;

    OnDisposable server =
        AeronServer.create(serverResources)
            .options("localhost", serverPort, serverControlPort)
            .bind()
            .block(TIMEOUT);

    // create the first client connection
    AeronConnection clientConnection1 =
        AeronClient.create(newClientResources())
            .options("localhost", serverPort, serverControlPort)
            .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
            .options(
                options ->
                    options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
            .connect()
            .block(TIMEOUT);

    // create the second client connection with the same session id
    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(
                    options ->
                        options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
                .connect())
        .expectError(TimeoutException.class)
        .verify(CONNECT_TIMEOUT.plusSeconds(1));
  }

  @Test
  public void testMultiClientWithTheSameSessionIdWtihRecovery() {
    OnDisposable server =
        AeronServer.create(serverResources)
            .options("localhost", serverPort, serverControlPort)
            .bind()
            .block(TIMEOUT);

    // create the first client connection
    AtomicInteger sessionIdCounterClient1 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient1 = sessionIdCounterClient1::getAndIncrement;

    AeronConnection clientConnection1 =
        AeronClient.create(newClientResources())
            .options("localhost", serverPort, serverControlPort)
            .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
            .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient1))
            .connect()
            .block(TIMEOUT);

    // create the second client connection
    AtomicInteger sessionIdCounterClient2 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient2 = sessionIdCounterClient2::getAndIncrement;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient2))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.plusSeconds(3));
  }

  private AeronResources newClientResources() {
    AeronResources resources = new AeronResources().useTmpDir().singleWorker().start().block();
    clientResources.add(resources);
    return resources;
  }
}
