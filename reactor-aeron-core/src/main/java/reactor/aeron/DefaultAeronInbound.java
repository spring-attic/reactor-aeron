package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// TODO entire INBOUND process is unclear: what to do with SereverHandler.handler? whois publisher?
// whois subscription? whois subscriber? whois doing onSubscribe? and etc etc
public final class DefaultAeronInbound implements AeronInbound, OnDisposable {

  private volatile ByteBufferFlux flux;
  // private volatile MessageSubscription subscription;

  /**
   * Starts inbound.
   *
   * @return success result
   */
  public Mono<Void> start() {
    return Mono.defer(
        () -> {
          DataMessageProcessor messageProcessor = new DataMessageProcessor();

          flux = new ByteBufferFlux(messageProcessor);

          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .dataSubscription(
                  name,
                  channel,
                  streamId,
                  messageProcessor,
                  eventLoop,
                  null,
                  image -> Optional.ofNullable(onCompleteHandler).ifPresent(Runnable::run))
              .doOnSuccess(
                  result -> {
                    messageProcessor.onSubscription(subscription);
                  })
              .then();
        });
  }

  @Override
  public Flux<ByteBuffer> receive() {
    return flux.takeUntilOther(onDispose());
  }

  @Override
  public void dispose() {
    if (subscription != null) {
      subscription.dispose();
    }
  }

  @Override
  public Mono<Void> onDispose() {
    return subscription != null ? subscription.onDispose() : Mono.empty();
  }

  @Override
  public boolean isDisposed() {
    return subscription != null && subscription.isDisposed();
  }
}
