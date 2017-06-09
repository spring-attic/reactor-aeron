package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * @author Anatoly Kadyshev
 */
public class AeronClientTest extends BaseAeronTest {

    private String serverChannel = "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort(13000);

    private String clientChannel = "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();


    @Test(expected = RuntimeException.class)
    public void testClientCouldNotConnectToServer() {
        AeronClient client = AeronClient.create();
        client.newHandler((inbound, outbound) -> Mono.never()).block();
    }

    @Test
    public void testClientReceivesSignalsFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server-1");

        addDisposable(server.newHandler((inbound, outbound) ->
                Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("hello1", "2", "3")
                        .log("server")))));

        ReplayProcessor<String> processor = ReplayProcessor.create();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client").subscribe(processor);
            return Mono.never();
        }));

        StepVerifier.create(processor)
                .expectNext("hello1", "2", "3")
                .expectNoEvent(Duration.ofMillis(10))
                .thenCancel()
                .verify();
    }

    @Test
    public void testTwoClientsReceiveDataFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server");
        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log())).subscribe();
            return Mono.never();
        }));

        ReplayProcessor<String> processor1 = ReplayProcessor.create();
        AeronClient client1 = createAeronClient("client-1");
        addDisposable(client1.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-1").subscribe(processor1);
            return Mono.never();
        }));

        ReplayProcessor<String> processor2 = ReplayProcessor.create();
        AeronClient client2 = createAeronClient("client-2");
        addDisposable(client2.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-2").subscribe(processor2);
            return Mono.never();
        }));

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
    public void testClientWith2HandlersReceiveData() {
        AeronServer server = createAeronServer("server-1");

        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log("server"))).subscribe();
            return Mono.never();
        }));

        ReplayProcessor<String> processor1 = ReplayProcessor.create();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-1").subscribe(processor1);
            return Mono.never();
        }));

        ReplayProcessor<String> processor2 = ReplayProcessor.create();
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-2").subscribe(processor2);
            return Mono.never();
        }));

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

    private AeronClient createAeronClient(String name) {
        return AeronClient.create(name, options -> {
                options.clientChannel(clientChannel);
                options.serverChannel(serverChannel);
        });
    }

    private AeronServer createAeronServer(String name) {
        return AeronServer.create(name, options ->
                options.serverChannel(serverChannel));
    }

}
