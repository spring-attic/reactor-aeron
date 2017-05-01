package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
        AtomicReference<StepVerifier.FirstStep<String>> stepRef1 = new AtomicReference<>();
        AeronServer server = AeronServer.create();
        CountDownLatch latch = new CountDownLatch(1);
        addDisposable(server.newHandler((inbound, outbound) -> {
            stepRef1.set(
                    StepVerifier.create(inbound.receive().asString().log("receive")));
            latch.countDown();
            return Mono.never();
        }));

        AeronClient client = AeronClient.create();
        addDisposable(client.newHandler((inbound, outbound) -> {
            outbound.send(AeronTestUtils.newByteBufferFlux("Hello", "world!").log("send")).then().subscribe();
            return Mono.never();
        }));

        latch.await(5, TimeUnit.SECONDS);
        stepRef1.get().expectNext("Hello", "world!");
    }

}