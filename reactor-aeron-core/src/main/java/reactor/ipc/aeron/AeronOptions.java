package reactor.ipc.aeron;

import io.aeron.Aeron;
import java.time.Duration;

/** Aeron options. */
public class AeronOptions {

  private static final String DEFAULT_SERVER_CHANNEL = "aeron:udp?endpoint=localhost:12000";

  private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

  private static final int DEFAULT_CONTROL_BACKPRESSURE_TIMEOUT_MILLIS =
      (int) Duration.ofSeconds(5).toMillis();

  private static final int DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS =
      (int) Duration.ofSeconds(5).toMillis();

  private String serverChannel = DEFAULT_SERVER_CHANNEL;

  private int serverStreamId = 1;

  private Aeron aeron;

  private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;

  private int controlBackpressureTimeoutMillis = DEFAULT_CONTROL_BACKPRESSURE_TIMEOUT_MILLIS;

  private int backpressureTimeoutMillis = DEFAULT_BACKPRESSURE_TIMEOUT_MILLIS;

  /**
   * Return connect tieout millis.
   *
   * @return connect timeout millis
   */
  public int connectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  /**
   * Setter for connect timeout millis.
   *
   * @param connectTimeoutMillis connect timeout millis
   */
  public void connectTimeoutMillis(int connectTimeoutMillis) {
    if (connectTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "connectTimeoutMillis > 0 expected, but got: " + connectTimeoutMillis);
    }

    this.connectTimeoutMillis = connectTimeoutMillis;
  }

  /**
   * Return control backpressure timeout millis.
   *
   * @return control backpressure timeout millis
   */
  public int controlBackpressureTimeoutMillis() {
    return controlBackpressureTimeoutMillis;
  }

  /**
   * Setter control backpressure timeout millis.
   *
   * @param controlBackpressureTimeoutMillis control backpressure timeout millis
   */
  public void controlBackpressureTimeoutMillis(int controlBackpressureTimeoutMillis) {
    if (controlBackpressureTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "controlBackpressureTimeoutMillis > 0 expected, but got: "
              + controlBackpressureTimeoutMillis);
    }

    this.controlBackpressureTimeoutMillis = controlBackpressureTimeoutMillis;
  }

  /**
   * Setter for server channel.
   *
   * @param serverChannel server channel
   */
  public void serverChannel(String serverChannel) {
    this.serverChannel = serverChannel;
  }

  /**
   * Returns server channel.
   *
   * @return server channel
   */
  public String serverChannel() {
    return serverChannel;
  }

  public int serverStreamId() {
    return serverStreamId;
  }

  /**
   * Setter for server stream id.
   *
   * @param serverStreamId server stream id
   */
  public void serverStreamId(int serverStreamId) {
    if (serverStreamId <= 0) {
      throw new IllegalArgumentException("serverStreamId > 0 expected, but got: " + serverStreamId);
    }

    this.serverStreamId = serverStreamId;
  }

  public Aeron getAeron() {
    return aeron;
  }

  /**
   * Setter for aeron.
   *
   * @param aeron aeron
   */
  public void aeron(Aeron aeron) {
    this.aeron = aeron;
  }

  /**
   * Returns backpressure timeout millis.
   *
   * @return backpressure timeout millis
   */
  public int backpressureTimeoutMillis() {
    return backpressureTimeoutMillis;
  }

  /**
   * Setter for backpressure timeout millis.
   *
   * @param backpressureTimeoutMillis backpressure timeout millis
   */
  public void backpressureTimeoutMillis(int backpressureTimeoutMillis) {
    if (backpressureTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "backpressureTimeoutMillis > 0 expected, but got: " + backpressureTimeoutMillis);
    }

    this.backpressureTimeoutMillis = backpressureTimeoutMillis;
  }
}
