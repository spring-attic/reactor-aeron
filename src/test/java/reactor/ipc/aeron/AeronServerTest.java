package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.subscriber.AssertSubscriber;

/**
 * @author Anatoly Kadyshev
 */
public class AeronServerTest extends BaseAeronTest {

    @Test
    public void testServerHandlerCanBeDisposed() throws InterruptedException {
        AeronServer aeronServer = AeronServer.create();
        addDisposable(aeronServer.newHandler((inbound, outbound) ->
                Mono.from(outbound.send(AeronTestUtils.newByteBufferFlux("1", "2", "3")))));
    }

    @Test
    public void testServerReceivesData() throws InterruptedException {
        AssertSubscriber<String> subscriber = AssertSubscriber.create();
        AeronServer server = AeronServer.create();
        addDisposable(server.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("receive").subscribe(subscriber);
            return Mono.never();
        }));

        AeronClient client = AeronClient.create();
        addDisposable(client.newHandler((inbound, outbound) -> {
            outbound.send(AeronTestUtils.newByteBufferFlux("Hello", "world!").log("send")).then().subscribe();
            return Mono.never();
        }));

        subscriber.awaitAndAssertNextValues("Hello", "world!");
    }

}