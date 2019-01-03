package reactor.aeron.server;

import io.aeron.Image;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.DataFragmentHandler;
import reactor.aeron.DataMessageProcessor;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.OnDisposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

final class AeronServerHandler implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private final AeronServerOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  // TODO think of more performant concurrent hashmap
  private final Map<Integer, SessionHandler> connections = new ConcurrentHashMap<>(32);

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronServerOptions options) {
    this.options = options;

    this.resources = options.resources();
    this.handler = options.handler();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, th -> logger.warn("AeronServerHandler disposed with error: " + th));
  }

  public Mono<OnDisposable> start() {
    return Mono.defer(
        () -> {
          DataMessageProcessor messageProcessor = new DataMessageProcessor();

          DataFragmentHandler fragmentHandler = new DataFragmentHandler(messageProcessor);

          return resources
              .subscription(
                  options.inboundUri().asString(),
                  fragmentHandler,
                  this::onImageAvailable /*accept new session*/,
                  this::onImageUnavailable /*remove session*/)
              .thenReturn(this);
        });
  }

  private void onImageUnavailable(Image image) {
    // TODO remove from hashmap of connections
  }

  private void onImageAvailable(Image image) {
    resources
        .publication(
            options.outboundUri().sessionId(image.sessionId()).asString(),
            options.connectTimeout(),
            options.backpressureTimeout())
        .doOnSuccess(
            publication -> {
              new SessionHandler();
            })
        .subscribe(
            null,
            ex -> logger.error("Unexpected exception occurred on creating server outbound: " + ex));
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
                connections
                    .values()
                    .stream()
                    .map(
                        client -> {
                          client.dispose();
                          return client.onDispose();
                        })
                    .collect(Collectors.toList())));
  }

  private class SessionHandler implements Connection {

    private final Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;
    private final DefaultAeronInbound inbound;
    private final String clientChannel;
    private final int clientSessionStreamId;
    private final int serverSessionStreamId;
    private final long connectRequestId;
    private final long sessionId;
    private final int clientControlStreamId;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private SessionHandler(
        String clientChannel,
        int clientSessionStreamId,
        int clientControlStreamId,
        long connectRequestId,
        long sessionId,
        int serverSessionStreamId) {
      this.clientSessionStreamId = clientSessionStreamId;
      this.clientChannel = clientChannel;
      this.outbound = new DefaultAeronOutbound(category, clientChannel, resources, options);
      this.connectRequestId = connectRequestId;
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
      this.clientControlStreamId = clientControlStreamId;
      this.inbound = new DefaultAeronInbound(category, resources);

      this.dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: " + th));
    }

    private Mono<? extends Connection> start() {
      return connect()
          .then(outbound.start(clientSessionStreamId))
          .then(inbound.start(serverChannel, serverSessionStreamId, this::dispose))
          .thenReturn(this)
          .doOnSuccess(connection -> handlers.add(this))
          .doOnError(
              ex -> {
                logger.error("Exception occurred on: {}, cause: {}", this, ex.toString());
                dispose();
              });
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
      return "ServerSession{"
          + "category="
          + category
          + ", sessionId="
          + sessionId
          + ", clientChannel="
          + AeronUtils.minifyChannel(clientChannel)
          + ", serverChannel="
          + AeronUtils.minifyChannel(serverChannel)
          + ", clientControlStreamId="
          + clientControlStreamId
          + ", clientSessionStreamId="
          + clientSessionStreamId
          + ", serverSessionStreamId="
          + serverSessionStreamId
          + '}';
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

    private Mono<Void> connect() {
      return Mono.defer(
          () -> {
            Duration retryInterval = Duration.ofMillis(100);
            Duration connectTimeout = options.connectTimeout().plus(options.backpressureTimeout());
            long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

            return controlPublication.flatMap(
                publication ->
                    sendConnectAck(publication)
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .doOnSuccess(
                            avoid ->
                                logger.debug("ServerSession sent CONNECT_ACK to: {}", publication))
                        .onErrorResume(
                            th -> {
                              logger.warn(
                                  "Failed to send CONNECT_ACK to: {}, cause: {}",
                                  publication,
                                  th.toString());
                              return Mono.error(
                                  new RuntimeException("Failed to send CONNECT_ACK", th));
                            }));
          });
    }
  }
}
