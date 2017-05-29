package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

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
        AeronServer server = AeronServer.create();
        ReplayProcessor<String> processor = ReplayProcessor.create();
        addDisposable(server.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("receive").subscribe(processor);
            return Mono.never();
        }));

        AeronClient client = AeronClient.create();
        addDisposable(client.newHandler((inbound, outbound) -> {
            outbound.send(AeronTestUtils.newByteBufferFlux("Hello", "world!").log("send")).then().subscribe();
            return Mono.never();
        }));

        StepVerifier.create(processor)
            .expectNext("Hello", "world!")
            .thenCancel()
            .verify();
    }

}