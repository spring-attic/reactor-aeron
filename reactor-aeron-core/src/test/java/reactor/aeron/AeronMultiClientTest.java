package reactor.aeron;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class AeronMultiClientTest extends BaseAeronTest {

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
            .options(
                options ->
                    options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
            .connect()
            .block(TIMEOUT);

    // create the second client connection with the same session id
    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(
                    options ->
                        options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
                .connect()
                .timeout(TIMEOUT.dividedBy(2)))
        .expectError()
        .verify(TIMEOUT);
  }

  private AeronResources newClientResources() {
    AeronResources resources = new AeronResources().useTmpDir().singleWorker().start().block();
    clientResources.add(resources);
    return resources;
  }
}
