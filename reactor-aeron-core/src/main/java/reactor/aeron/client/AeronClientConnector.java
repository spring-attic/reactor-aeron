package reactor.aeron.client;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public final class AeronClientConnector implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientConnector.class);

  private final AeronOptions options;
  private final AeronResources resources;

  private final List<ClientHandler> handlers = new CopyOnWriteArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronClientConnector(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, th -> logger.warn("AeronClientConnector disposed with error: " + th));
  }

  /**
   * Creates {@link ClientHandler} object and starts it. See for details method {@link
   * ClientHandler#start()}.
   */
  public Mono<Connection> start() {
    return Mono.defer(() -> new ClientHandler().start());
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
        () ->
            Mono.whenDelayError(
                handlers
                    .stream()
                    .map(
                        sessionHandler -> {
                          sessionHandler.dispose();
                          return sessionHandler.onDispose();
                        })
                    .collect(Collectors.toList())));
  }

  private static class ClientHandler implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final DefaultAeronOutbound outbound;
    private final String serverChannel;

    private volatile DefaultAeronInbound inbound;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private ClientHandler() {
      serverChannel = options.serverChannel();
      inbound = new DefaultAeronInbound();
      outbound = new DefaultAeronOutbound(serverChannel, resources, options);

      dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(null, th -> logger.warn("ClientHandler disposed with error: " + th));
    }

    private Mono<? extends Connection> start() {
      handlers.add(this);

      return connect()
          .flatMap(
              response -> {
                sessionId = response.sessionId;
                serverSessionStreamId = response.serverSessionStreamId;

                return inbound.start().then(outbound.start(serverSessionStreamId)).thenReturn(this);
              })
          .doOnError(
              ex -> {
                logger.error("Exception occurred on: {}, cause: {}", this, ex.toString());
                dispose();
              })
          .thenReturn(this);
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
            logger.debug("About to close {}", this);

            handlers.remove(this);

            Optional.ofNullable(outbound) //
                .ifPresent(DefaultAeronOutbound::dispose);
            Optional.ofNullable(inbound) //
                .ifPresent(DefaultAeronInbound::dispose);

            return Mono.whenDelayError(
                    Optional.ofNullable(outbound)
                        .map(DefaultAeronOutbound::onDispose)
                        .orElse(Mono.empty()),
                    Optional.ofNullable(inbound)
                        .map(DefaultAeronInbound::onDispose)
                        .orElse(Mono.empty()))
                .doFinally(s -> logger.debug("Closed {}", this));
          });
    }
  }
}
