package reactor.aeron;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/** Immutable wrapper around properties of {@link ChannelUriStringBuilder}. */
public class AeronChannelUri {

  private String address; // address
  private Integer port; // .. and port
  private String endpoint; // endpoint right away (preferred if set)
  private String controlAddress; // control address
  private Integer controlPort; // .. and port
  private String controlEndpoint; // control endpoint right away (preferred if set)
  private Integer mtu; // zero
  private String controlMode = CommonContext.MDC_CONTROL_MODE_DYNAMIC; // dynamic
  private Integer sessionId;
  private String media = CommonContext.UDP_MEDIA; // udp
  private Boolean reliable = true; // reliable is true

  public AeronChannelUri() {}

  public AeronChannelUri(AeronChannelUri other) {
    this.address = other.address;
    this.port = other.port;
    this.endpoint = other.endpoint;
    this.controlAddress = other.controlAddress;
    this.controlPort = other.controlPort;
    this.controlEndpoint = other.controlEndpoint;
    this.mtu = other.mtu;
    this.controlMode = other.controlMode;
    this.sessionId = other.sessionId;
    this.media = other.media;
    this.reliable = other.reliable;
  }

  public String endpoint() {
    return Optional.ofNullable(endpointOrNull())
        .orElseThrow(() -> new IllegalStateException("endpoint must be set"));
  }

  public String controlEndpoint() {
    return Optional.ofNullable(controlEndpointOrNull())
        .orElseThrow(() -> new IllegalStateException("controlEndpoint must be set"));
  }

  public Integer mtu() {
    return mtu;
  }

  public String controlMode() {
    return controlMode;
  }

  public Integer sessionId() {
    return sessionId;
  }

  public String media() {
    return media;
  }

  public Boolean reliable() {
    return reliable;
  }

  public AeronChannelUri endpoint(String endpoint) {
    return set(u -> u.endpoint = Objects.requireNonNull(endpoint));
  }

  public AeronChannelUri endpoint(String address, Integer port) {
    return set(
        u -> {
          u.address = Objects.requireNonNull(address);
          u.port = Objects.requireNonNull(port);
        });
  }

  public AeronChannelUri controlEndpoint(String controlEndpoint) {
    return set(u -> u.controlEndpoint = Objects.requireNonNull(controlEndpoint));
  }

  public AeronChannelUri controlAddress(String controlAddress, Integer controlPort) {
    return set(
        u -> {
          u.controlAddress = Objects.requireNonNull(controlAddress);
          u.controlPort = Objects.requireNonNull(controlPort);
        });
  }

  public AeronChannelUri mtu(Integer mtu) {
    return set(u -> u.mtu = mtu);
  }

  public AeronChannelUri controlMode(String controlMode) {
    return set(u -> u.controlMode = controlMode);
  }

  public AeronChannelUri sessionId(Integer sessionId) {
    return set(u -> u.sessionId = sessionId);
  }

  public AeronChannelUri media(String media) {
    return set(u -> u.media = media);
  }

  public AeronChannelUri reliable(Boolean reliable) {
    return set(u -> u.reliable = reliable);
  }

  private AeronChannelUri set(Consumer<AeronChannelUri> c) {
    AeronChannelUri u = new AeronChannelUri(this);
    c.accept(u);
    return u;
  }

  private String endpointOrNull() {
    return Optional.ofNullable(endpoint)
        .orElseGet(
            () ->
                Optional.ofNullable(address)
                    .flatMap(a -> Optional.ofNullable(port).map(p -> a + ':' + p))
                    .orElse(null));
  }

  private String controlEndpointOrNull() {
    return Optional.ofNullable(controlEndpoint)
        .orElseGet(
            () ->
                Optional.ofNullable(controlAddress)
                    .flatMap(a -> Optional.ofNullable(controlPort).map(p -> a + ':' + p))
                    .orElse(null));
  }

  /**
   * Delegates to {@link ChannelUriStringBuilder#build()}.
   *
   * @return aeron channel string
   */
  public String asString() {
    ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
    Optional.ofNullable(endpointOrNull()).ifPresent(builder::endpoint);
    Optional.ofNullable(controlEndpointOrNull()).ifPresent(builder::controlEndpoint);
    Optional.ofNullable(controlMode()).ifPresent(builder::controlMode);
    Optional.ofNullable(mtu()).ifPresent(builder::mtu);
    Optional.ofNullable(sessionId()).ifPresent(builder::sessionId);
    Optional.ofNullable(media()).ifPresent(builder::media);
    Optional.ofNullable(reliable()).ifPresent(builder::reliable);
    return builder.build();
  }

  @Override
  public String toString() {
    return "AeronChannelUri{"
        + "endpoint="
        + endpointOrNull()
        + ", controlEndpoint="
        + controlEndpointOrNull()
        + ", controlMode="
        + controlMode()
        + ", mtu="
        + mtu()
        + ", sessionId="
        + sessionId()
        + ", media="
        + media()
        + ", reliable="
        + reliable()
        + '}';
  }
}
