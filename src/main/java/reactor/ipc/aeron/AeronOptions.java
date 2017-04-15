package reactor.ipc.aeron;

import uk.co.real_logic.aeron.Aeron;

import java.time.Duration;

/**
 * @author Anatoly Kadyshev
 */
public class AeronOptions {

    private static final String DEFAULT_SERVER_CHANNEL = "udp://localhost:12000";

    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

    private static final int DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

    private String serverChannel = DEFAULT_SERVER_CHANNEL;

    private int serverStreamId = 1;

    private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;

    private Aeron aeron;

    private int backpressureTimeoutMillis = DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS;

    public int connectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void connectTimeoutMillis(int connectTimeoutMillis) {
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis > 0 expected, but got: " + connectTimeoutMillis);
        }

        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void serverChannel(String serverChannel) {
        this.serverChannel = serverChannel;
    }

    public String serverChannel() {
        return serverChannel;
    }

    public int serverStreamId() {
        return serverStreamId;
    }

    public void serverStreamId(int serverStreamId) {
        if (serverStreamId <= 0) {
            throw new IllegalArgumentException("serverStreamId > 0 expected, but got: " + serverStreamId);
        }

        this.serverStreamId = serverStreamId;
    }

    public Aeron getAeron() {
        return aeron;
    }

    public void aeron(Aeron aeron) {
        this.aeron = aeron;
    }

    public int backpressureTimeoutMillis() {
        return backpressureTimeoutMillis;
    }

    public void backpressureTimeoutMillis(int backpressureTimeoutMillis) {
        if (backpressureTimeoutMillis <= 0) {
            throw new IllegalArgumentException("backpressureTimeoutMillis > 0 expected, but got: " + backpressureTimeoutMillis);
        }

        this.backpressureTimeoutMillis = backpressureTimeoutMillis;
    }
}
