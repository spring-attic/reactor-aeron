package reactor.ipc.aeron.demo;

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.ByteBufferFlux;
import reactor.ipc.aeron.client.AeronClient;

/**
 * @author Anatoly Kadyshev
 */
public class ClientDemo {

    public static void main(String[] args) {
        AeronClient client = AeronClient.create("client", options -> {
            options.serverChannel("aeron:udp?endpoint=localhost:13000");
            options.clientChannel("aeron:udp?endpoint=localhost:12001");
        });
        client.newHandler((inbound, outbound) -> {
            System.out.println("Handler invoked");
            outbound.send(ByteBufferFlux.from("Hello", "world!").log("send"))
                    .then().subscribe(avoid -> {}, Throwable::printStackTrace);
            return Mono.never();
        }).block();

        System.out.println("main completed");
    }

}
