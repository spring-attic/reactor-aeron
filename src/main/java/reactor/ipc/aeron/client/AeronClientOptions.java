package reactor.ipc.aeron.client;

import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.SocketUtils;

import java.util.Objects;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClientOptions extends AeronOptions {

    private String clientChannel = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

    public String clientChannel() {
        return clientChannel;
    }

    public void clientChannel(String clientChannel) {
        Objects.requireNonNull(clientChannel, "clientChannel");

        this.clientChannel = clientChannel;
    }
}
