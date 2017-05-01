package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Anatoly Kadyshev
 */
public class AeronClientTest extends BaseAeronTest {

    private String serverChannel = "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();

    @Test(expected = RuntimeException.class)
    public void testClientCouldNotConnectToServer() {
        AeronClient client = AeronClient.create();
        client.newHandler((inbound, outbound) -> Mono.never()).block();
    }

    @Test
    public void testClientReceivesSignalsFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server-1");

        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log("server"))).subscribe();
            return Mono.never();
        }));

        AtomicReference<StepVerifier.FirstStep<String>> stepRef = new AtomicReference<>();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            stepRef.set(StepVerifier.create(
                    inbound.receive().map(AeronUtils::byteBufferToString).log("client")));
            return Mono.never();
        }));

        stepRef.get().expectNext("1", "2", "3");
    }

    @Test
    public void testTwoClientsReceiveDataFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server");
        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log())).subscribe();
            return Mono.never();
        }));

        AtomicReference<StepVerifier.FirstStep<String>> stepRef1 = new AtomicReference<>();
        AeronClient client1 = createAeronClient("client-1");
        addDisposable(client1.newHandler((inbound, outbound) -> {
            stepRef1.set(StepVerifier.create(
                    inbound.receive().map(AeronUtils::byteBufferToString).log("client-1")));
            return Mono.never();
        }));

        AtomicReference<StepVerifier.FirstStep<String>> stepRef2 = new AtomicReference<>();
        AeronClient client2 = createAeronClient("client-2");
        addDisposable(client2.newHandler((inbound, outbound) -> {
            stepRef2.set(StepVerifier.create(
                    inbound.receive().map(AeronUtils::byteBufferToString).log("client-2")));
            return Mono.never();
        }));

        stepRef1.get().expectNext("1", "2", "3");
        stepRef2.get().expectNext("1", "2", "3");
    }

    @Test
    public void testClientWith2HandlersReceiveData() {
        AeronServer server = createAeronServer("server-1");

        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log("server"))).subscribe();
            return Mono.never();
        }));

        AtomicReference<StepVerifier.FirstStep<String>> stepRef1 = new AtomicReference<>();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            stepRef1.set(StepVerifier.create(
                    inbound.receive().map(AeronUtils::byteBufferToString).log("client-1")));
            return Mono.never();
        }));

        AtomicReference<StepVerifier.FirstStep<String>> stepRef2 = new AtomicReference<>();
        addDisposable(client.newHandler((inbound, outbound) -> {
            stepRef2.set(StepVerifier.create(
                inbound.receive().map(AeronUtils::byteBufferToString).log("client-2")));
            return Mono.never();
        }));

        StepVerifier.FirstStep<String> step1 = stepRef1.get();
        step1.expectNext("1", "2", "3");
        step1.expectNextCount(3);
        StepVerifier.FirstStep<String> step2 = stepRef2.get();
        step2.expectNext("1", "2", "3");
        step2.expectNextCount(3);
    }

    private AeronClient createAeronClient(String name) {
        return AeronClient.create(name, options ->
                options.serverChannel(serverChannel));
    }

    private AeronServer createAeronServer(String name) {
        return AeronServer.create(name, options ->
                options.serverChannel(serverChannel));
    }

}
