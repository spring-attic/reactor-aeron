package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.subscriber.AssertSubscriber;

/**
 * @author Anatoly Kadyshev
 */
public class AeronClientTest extends BaseAeronTest {

    private String serverChannel = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

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

        AssertSubscriber<String> subscriber = AssertSubscriber.create();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client").subscribe(subscriber);
            return Mono.never();
        }));

        subscriber.awaitAndAssertNextValues("1", "2", "3");
    }

    @Test
    public void testTwoClientsReceiveDataFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server");
        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log())).subscribe();
            return Mono.never();
        }));

        AssertSubscriber<String> subscriber1 = AssertSubscriber.create();
        AeronClient client1 = createAeronClient("client-1");
        addDisposable(client1.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-1").subscribe(subscriber1);
            return Mono.never();
        }));

        AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
        AeronClient client2 = createAeronClient("client-2");
        addDisposable(client2.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-2").subscribe(subscriber2);
            return Mono.never();
        }));

        subscriber1.awaitAndAssertNextValues("1", "2", "3");
        subscriber2.awaitAndAssertNextValues("1", "2", "3");
    }

    @Test
    public void testClientWith2HandlersReceiveData() {
        AeronServer server = createAeronServer("server-1");

        addDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3").log("server"))).subscribe();
            return Mono.never();
        }));

        AssertSubscriber<String> subscriber1 = AssertSubscriber.create();
        AeronClient client = createAeronClient(null);
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-1").subscribe(subscriber1);
            return Mono.never();
        }));

        AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
        addDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().map(AeronUtils::byteBufferToString).log("client-2").subscribe(subscriber2);
            return Mono.never();
        }));

        subscriber1.awaitAndAssertNextValues("1", "2", "3");
        subscriber1.assertValueCount(3);
        subscriber2.awaitAndAssertNextValues("1", "2", "3");
        subscriber2.assertValueCount(3);
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
