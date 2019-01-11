package reactor.aeron;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Immutable wrapper around properties of {@link ChannelUriStringBuilder}. All fields from aeron
 * builder are covered.
 */
public final class AeronChannelUri {

  private ChannelUriStringBuilder builder =
      new ChannelUriStringBuilder().media(CommonContext.UDP_MEDIA).reliable(true);

  public AeronChannelUri() {}

  /**
   * Copy constructor.
   *
   * @param other instance to copy from
   */
  public AeronChannelUri(AeronChannelUri other) {
    builder
        .endpoint(other.builder.endpoint())
        .controlEndpoint(other.builder.controlEndpoint())
        .mtu(other.builder.mtu())
        .controlMode(other.builder.controlMode())
        .sessionId(other.builder.sessionId())
        .media(other.builder.media())
        .reliable(other.builder.reliable())
        .ttl(other.builder.ttl())
        .initialTermId(other.builder.initialTermId())
        .termOffset(other.builder.termOffset())
        .termLength(other.builder.termLength())
        .termId(other.builder.termId())
        .linger(other.builder.linger())
        .networkInterface(other.builder.networkInterface());
  }

  public String endpoint() {
    return builder.endpoint();
  }

  public AeronChannelUri endpoint(String endpoint) {
    return set(u -> u.builder.endpoint(endpoint));
  }

  public String controlEndpoint() {
    return builder.controlEndpoint();
  }

  public AeronChannelUri controlEndpoint(String controlEndpoint) {
    return set(u -> u.builder.controlEndpoint(controlEndpoint));
  }

  public Integer mtu() {
    return builder.mtu();
  }

  public AeronChannelUri mtu(Integer mtu) {
    return set(u -> u.builder.mtu(mtu));
  }

  public String controlMode() {
    return builder.controlMode();
  }

  public AeronChannelUri controlModeDynamic() {
    return set(u -> u.builder.controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC));
  }

  public AeronChannelUri controlModeManual() {
    return set(u -> u.builder.controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL));
  }

  public Integer sessionId() {
    return builder.sessionId();
  }

  public AeronChannelUri sessionId(Integer sessionId) {
    return set(u -> u.builder.sessionId(sessionId));
  }

  public String media() {
    return builder.media();
  }

  public AeronChannelUri media(String media) {
    return set(u -> u.builder.media(media));
  }

  public Boolean reliable() {
    return builder.reliable();
  }

  public AeronChannelUri reliable(Boolean reliable) {
    return set(u -> u.builder.reliable(reliable));
  }

  public Integer ttl() {
    return builder.ttl();
  }

  public AeronChannelUri ttl(Integer ttl) {
    return set(u -> u.builder.ttl(ttl));
  }

  public Integer termOffset() {
    return builder.termOffset();
  }

  public AeronChannelUri termOffset(Integer termOffset) {
    return set(u -> u.builder.termOffset(termOffset));
  }

  public Integer termLength() {
    return builder.termLength();
  }

  public AeronChannelUri termLength(Integer termLength) {
    return set(u -> u.builder.termLength(termLength));
  }

  public Integer termId() {
    return builder.termId();
  }

  public AeronChannelUri termId(Integer termId) {
    return set(u -> u.builder.termId(termId));
  }

  public String networkInterface() {
    return builder.networkInterface();
  }

  public AeronChannelUri networkInterface(String networkInterface) {
    return set(u -> u.builder.networkInterface(networkInterface));
  }

  public Integer linger() {
    return builder.linger();
  }

  public AeronChannelUri linger(Integer linger) {
    return set(u -> u.builder.linger(linger));
  }

  public Integer initialTermId() {
    return builder.initialTermId();
  }

  public AeronChannelUri initialTermId(Integer initialTermId) {
    return set(u -> u.builder.initialTermId(initialTermId));
  }

  public AeronChannelUri initialPosition(long position, int initialTermId, int termLength) {
    return set(u -> u.builder.initialPosition(position, initialTermId, termLength));
  }

  private AeronChannelUri set(Consumer<AeronChannelUri> c) {
    AeronChannelUri u = new AeronChannelUri(this);
    c.accept(u);
    return u;
  }

  /**
   * Delegates to {@link ChannelUriStringBuilder#build()}.
   *
   * @return aeron channel string
   */
  public String asString() {
    ChannelUriStringBuilder builder = new ChannelUriStringBuilder();
    Optional.ofNullable(endpoint()).ifPresent(builder::endpoint);
    Optional.ofNullable(controlEndpoint()).ifPresent(builder::controlEndpoint);
    Optional.ofNullable(controlMode()).ifPresent(builder::controlMode);
    Optional.ofNullable(mtu()).ifPresent(builder::mtu);
    Optional.ofNullable(sessionId()).ifPresent(builder::sessionId);
    Optional.ofNullable(media()).ifPresent(builder::media);
    Optional.ofNullable(reliable()).ifPresent(builder::reliable);
    Optional.ofNullable(ttl()).ifPresent(builder::ttl);
    Optional.ofNullable(initialTermId()).ifPresent(builder::initialTermId);
    Optional.ofNullable(termOffset()).ifPresent(builder::termOffset);
    Optional.ofNullable(termLength()).ifPresent(builder::termLength);
    Optional.ofNullable(termId()).ifPresent(builder::termId);
    Optional.ofNullable(networkInterface()).ifPresent(builder::networkInterface);
    Optional.ofNullable(linger()).ifPresent(builder::linger);
    return builder.build();
  }

  @Override
  public String toString() {
    return "AeronChannelUri{" + asString() + "}";
  }
}
