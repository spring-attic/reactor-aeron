package reactor.aeron;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Immutable wrapper around properties of {@link ChannelUriStringBuilder}. All fields from aeron
 * builder are covered.
 */
public final class AeronChannelUri {

  private String endpoint; // endpoint right away (preferred if set)
  private String controlEndpoint; // control endpoint right away (preferred if set)
  private Integer mtu; // zero
  private String controlMode = CommonContext.MDC_CONTROL_MODE_DYNAMIC; // dynamic
  private Integer sessionId;
  private String media = CommonContext.UDP_MEDIA; // udp
  private Boolean reliable = true; // reliable is true
  private Integer ttl; // multicast setting
  private Integer termOffset;
  private Integer termLength;
  private Integer termId;
  private String tags;
  private Boolean sparse;
  private String prefix;
  private String networkInterface;
  private Duration linger;
  private Boolean sessionIdTagged;
  private Integer initialTermId;

  public AeronChannelUri() {}

  /**
   * Copy constructor.
   *
   * @param other instance to copy from
   */
  public AeronChannelUri(AeronChannelUri other) {
    this.endpoint = other.endpoint;
    this.controlEndpoint = other.controlEndpoint;
    this.mtu = other.mtu;
    this.controlMode = other.controlMode;
    this.sessionId = other.sessionId;
    this.media = other.media;
    this.reliable = other.reliable;
    this.ttl = other.ttl;
    this.termOffset = other.termOffset;
    this.termLength = other.termLength;
    this.termId = other.termId;
    this.tags = other.tags;
    this.sparse = other.sparse;
    this.prefix = other.prefix;
    this.networkInterface = other.networkInterface;
    this.linger = other.linger;
    this.sessionIdTagged = other.sessionIdTagged;
    this.initialTermId = other.initialTermId;
  }

  public String endpoint() {
    return endpoint;
  }

  public AeronChannelUri endpoint(String endpoint) {
    return set(u -> u.endpoint = Objects.requireNonNull(endpoint));
  }

  /**
   * Set endpoint with given {@code address} and {@code port}.
   *
   * @param address address
   * @param port port
   * @return new {@code AeronChannelUri}
   */
  public AeronChannelUri endpoint(String address, Integer port) {
    Objects.requireNonNull(address);
    Objects.requireNonNull(port);
    return endpoint(address + ':' + port);
  }

  public String controlEndpoint() {
    return controlEndpoint;
  }

  public AeronChannelUri controlEndpoint(String controlEndpoint) {
    return set(u -> u.controlEndpoint = Objects.requireNonNull(controlEndpoint));
  }

  /**
   * Set control-endpoint with given {@code address} and {@code port}.
   *
   * @param address address
   * @param port port
   * @return new {@code AeronChannelUri}
   */
  public AeronChannelUri controlEndpoint(String address, Integer port) {
    Objects.requireNonNull(address);
    Objects.requireNonNull(port);
    return controlEndpoint(address + ':' + port);
  }

  public Integer mtu() {
    return mtu;
  }

  public AeronChannelUri mtu(Integer mtu) {
    return set(u -> u.mtu = mtu);
  }

  public String controlMode() {
    return controlMode;
  }

  public AeronChannelUri controlMode(String controlMode) {
    return set(u -> u.controlMode = controlMode);
  }

  public Integer sessionId() {
    return sessionId;
  }

  public AeronChannelUri sessionId(Integer sessionId) {
    return set(u -> u.sessionId = sessionId);
  }

  public String media() {
    return media;
  }

  public AeronChannelUri media(String media) {
    return set(u -> u.media = media);
  }

  public Boolean reliable() {
    return reliable;
  }

  public AeronChannelUri reliable(Boolean reliable) {
    return set(u -> u.reliable = reliable);
  }

  public Integer ttl() {
    return ttl;
  }

  public AeronChannelUri ttl(Integer ttl) {
    return set(u -> u.ttl = ttl);
  }

  public Integer termOffset() {
    return termOffset;
  }

  public AeronChannelUri termOffset(Integer termOffset) {
    return set(u -> u.termOffset = termOffset);
  }

  public Integer termLength() {
    return termLength;
  }

  public AeronChannelUri termLength(Integer termLength) {
    return set(u -> u.termLength = termLength);
  }

  public Integer termId() {
    return termId;
  }

  public AeronChannelUri termId(Integer termId) {
    return set(u -> u.termId = termId);
  }

  public String tags() {
    return tags;
  }

  public AeronChannelUri tags(String tags) {
    return set(u -> u.tags = tags);
  }

  public Boolean sparse() {
    return sparse;
  }

  public AeronChannelUri sparse(Boolean sparse) {
    return set(u -> u.sparse = sparse);
  }

  public String prefix() {
    return prefix;
  }

  public AeronChannelUri prefix(String prefix) {
    return set(u -> u.prefix = prefix);
  }

  public String networkInterface() {
    return networkInterface;
  }

  public AeronChannelUri networkInterface(String networkInterface) {
    return set(u -> u.networkInterface = networkInterface);
  }

  public Duration linger() {
    return linger;
  }

  public AeronChannelUri linger(Duration linger) {
    return set(u -> u.linger = linger);
  }

  public Boolean sessionIdTagged() {
    return sessionIdTagged;
  }

  public AeronChannelUri sessionIdTagged(Boolean sessionIdTagged) {
    return set(u -> u.sessionIdTagged = sessionIdTagged);
  }

  public Integer initialTermId() {
    return initialTermId;
  }

  public AeronChannelUri initialTermId(Integer initialTermId) {
    return set(u -> u.initialTermId = initialTermId);
  }

  /**
   * Delegates to {@link ChannelUriStringBuilder#initialPosition(long, int, int)}.
   *
   * @param position position
   * @param initialTermId initialTermId
   * @param termLength termLength
   * @return new {@code AeronChannelUri}
   */
  public AeronChannelUri initialPosition(long position, int initialTermId, int termLength) {
    return set(
        u -> {
          ChannelUriStringBuilder b =
              new ChannelUriStringBuilder().initialPosition(position, initialTermId, termLength);
          u.initialTermId = b.initialTermId();
          u.termId = b.termId();
          u.termOffset = b.termOffset();
          u.termLength = b.termLength();
        });
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
    Optional.ofNullable(termOffset()).ifPresent(builder::termOffset);
    Optional.ofNullable(termLength()).ifPresent(builder::termLength);
    Optional.ofNullable(termId()).ifPresent(builder::termId);
    Optional.ofNullable(tags()).ifPresent(builder::tags);
    Optional.ofNullable(sparse()).ifPresent(builder::sparse);
    Optional.ofNullable(prefix()).ifPresent(builder::prefix);
    Optional.ofNullable(networkInterface()).ifPresent(builder::networkInterface);
    Optional.ofNullable(linger())
        .map(duration -> (int) duration.toNanos())
        .ifPresent(builder::linger);
    Optional.ofNullable(sessionIdTagged()).ifPresent(builder::isSessionIdTagged);
    Optional.ofNullable(initialTermId()).ifPresent(builder::initialTermId);
    return builder.build();
  }

  @Override
  public String toString() {
    return "AeronChannelUri{" + asString() + "}";
  }
}
