package reactor.aeron;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron <i>connection</i>. Bound to certain {@code sessionId}. Implements {@link
 * OnDisposable} for convenient resource cleanup.
 */
public final class DefaultAeronConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(DefaultAeronConnection.class);

  private final int sessionId;

  private final DefaultAeronInbound inbound;
  private final DefaultAeronOutbound outbound;

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param sessionId session id
   * @param inbound inbound
   * @param outbound outbound
   */
  public DefaultAeronConnection(
      int sessionId, DefaultAeronInbound inbound, DefaultAeronOutbound outbound) {
    this.sessionId = sessionId;
    this.inbound = inbound;
    this.outbound = outbound;

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th ->
                logger.warn(
                    "{}: aeron connection disposed with error: {}", sessionId, th.toString()));
  }

  @Override
  public AeronInbound inbound() {
    return inbound;
  }

  @Override
  public AeronOutbound outbound() {
    return outbound;
  }

  @Override
  public String toString() {
    return ""; // TODO implement
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.defer(
        () -> {
          logger.debug("{}: is disposing {}", Integer.toHexString(sessionId), this);
          return Mono.whenDelayError(
                  Mono.fromRunnable(inbound::dispose).then(inbound.onDispose()),
                  Mono.fromRunnable(outbound::dispose).then(outbound.onDispose()))
              .doFinally(
                  s -> logger.debug("{}: disposed {}", Integer.toHexString(sessionId), this));
        });
  }
}
