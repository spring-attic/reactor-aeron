package reactor.ipc.aeron.demo;

import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;

/**
 * @author Anatoly Kadyshev
 */
public class Client_ServerSends {

    public static void main(String[] args) {
        AeronClient client = AeronClient.create("client", options -> {
            options.serverChannel("aeron:udp?endpoint=localhost:13000");
            options.clientChannel("aeron:udp?endpoint=localhost:12001");
            options.connectTimeoutMillis(5000);
        });
        client.newHandler((inbound, outbound) -> {
            System.out.println("Handler invoked");
            inbound.receive().asString().log("receive").subscribe();
            return Mono.never();
        }).block();

        System.out.println("main completed");
    }

}
