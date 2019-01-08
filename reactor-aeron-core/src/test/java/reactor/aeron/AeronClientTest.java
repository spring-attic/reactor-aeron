package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.aeron.driver.Configuration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.aeron.client.AeronClient;
import reactor.aeron.server.AeronServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

class AeronClientTest extends BaseAeronTest {

  private int serverPort;
  private int serverControlPort;
  private AeronResources clientResources;
  private AeronResources serverResources;

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    clientResources = AeronResources.start(AeronResourcesConfig.builder().numOfWorkers(1).build());
    serverResources = AeronResources.start(AeronResourcesConfig.builder().numOfWorkers(1).build());
  }

  @AfterEach
  void afterEach() {
    if (clientResources != null) {
      clientResources.dispose();
      clientResources.onDispose().block(TIMEOUT);
    }
    if (serverResources != null) {
      serverResources.dispose();
      serverResources.onDispose().block(TIMEOUT);
    }
  }

  @Test
  public void testClientReceivesDataFromServer() {
    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.fromString("hello1", "2", "3").log("server"))
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
    char[] chars = new char[Configuration.MTU_LENGTH * 2];
    Arrays.fill(chars, 'a');
    String str = new String(chars);

    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.fromString(str, str, str).log("server"))
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
                .send(ByteBufferFlux.fromString("1", "2", "3").log("server"))
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
  public void testClientsReceiveDataFromServer200000() {
    int count = 200_000;
    Flux<String> payloads = Flux.range(0, count).map(String::valueOf);

    createServer(
        connection -> connection.outbound().sendString(payloads).then(connection.onDispose()));

    Connection connection1 = createConnection();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testRequestResponse200000() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .outbound()
                .send(connection.inbound().receive())
                .then(connection.onDispose()));

    Connection connection1 = createConnection();

    connection1.outbound().sendString(Flux.range(0, count).map(String::valueOf)).then().subscribe();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testRequestResponse200000MonoJust() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .flatMap(byteBuffer -> connection.outbound().send(Mono.just(byteBuffer)).then())
                .then(connection.onDispose()));

    Connection connection1 = createConnection();

    Flux.range(0, count)
        .flatMap(
            i -> connection1.outbound().send(ByteBufferFlux.fromString("client_send:" + i)).then())
        .then()
        .subscribe();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify(Duration.ofSeconds(20));
  }

  @Test
  public void testTwoClientsRequestResponse200000() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .flatMap(byteBuffer -> connection.outbound().send(Mono.just(byteBuffer)).then())
                .then(connection.onDispose()));

    ReplayProcessor<String> processor1 = ReplayProcessor.create();
    ReplayProcessor<String> processor2 = ReplayProcessor.create();

    createConnection(
        connection -> {
          connection.inbound().receive().asString().subscribe(processor1);
          Flux.range(0, count)
              .flatMap(
                  i -> connection.outbound().sendString(Mono.just("client-1 send:" + i)).then())
              .then()
              .subscribe();
          return connection.onDispose();
        });

    createConnection(
        connection -> {
          connection.inbound().receive().asString().subscribe(processor2);
          Flux.range(0, count)
              .flatMap(
                  i -> connection.outbound().sendString(Mono.just("client-2 send:" + i)).then())
              .then()
              .subscribe();
          return connection.onDispose();
        });

    StepVerifier.create(
            Flux.merge(
                processor1.take(count).filter(response -> !response.startsWith("client-1 ")),
                processor2.take(count).filter(response -> !response.startsWith("client-2 "))))
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testClientWith2HandlersReceiveData() {
    createServer(
        connection ->
            connection
                .outbound()
                .send(ByteBufferFlux.fromString("1", "2", "3").log("server"))
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
  public void testConcurrentSendingStreams() {
    int streams = 4;
    int overallCount = Queues.SMALL_BUFFER_SIZE * streams *2;
    int requestPerStream = overallCount / streams;

    ReplayProcessor<String> clientRequests = ReplayProcessor.create();

    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .asString()
                .doOnNext(clientRequests::onNext)
                // .log("server receive ")
                .then(connection.onDispose()));

    createConnection(
        connection -> {
          for (int i = 0; i < streams; i++) {
            int start = i * requestPerStream;
            int count = start + requestPerStream;
            connection
                .outbound()
                .sendString(Flux.range(start, count).map(String::valueOf))
                .then()
                .subscribe(null, Throwable::printStackTrace);
          }
          return connection.onDispose();
        });

    List<Integer> requests =
        clientRequests
            .take(requestPerStream * streams)
            .map(Integer::parseInt)
            .collectList()
            .block(Duration.ofSeconds(10));

    //noinspection ConstantConditions
    ArrayList<Integer> sortedRequests = new ArrayList<>(requests);
    Collections.sort(sortedRequests);
    assertNotEquals(requests, sortedRequests);
  }

  private Connection createConnection() {
    return createConnection(null /*handler*/);
  }

  private Connection createConnection(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return AeronClient.create(clientResources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
        .connect()
        .block(TIMEOUT);
  }

  private OnDisposable createServer(
      Function<? super Connection, ? extends Publisher<Void>> handler) {
    return AeronServer.create(serverResources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
        .bind()
        .block(TIMEOUT);
  }
}
