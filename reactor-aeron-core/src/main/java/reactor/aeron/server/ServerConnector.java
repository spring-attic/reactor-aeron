package reactor.aeron.server;

import io.aeron.Publication;
import java.time.Duration;
import java.util.UUID;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.DefaultMessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.Protocol;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public class ServerConnector implements Disposable {

  private static final Logger logger = Loggers.getLogger(ServerConnector.class);

  private final String category;

  private final Publication clientControlPublication;

  private final int serverSessionStreamId;

  private final UUID connectRequestId;

  private final AeronOptions options;

  private final long sessionId;

  private final AeronResources aeronResources;

  ServerConnector(
      String category,
      AeronResources aeronResources,
      String clientChannel,
      int clientControlStreamId,
      long sessionId,
      int serverSessionStreamId,
      UUID connectRequestId,
      AeronOptions options) {
    this.category = category;
    this.serverSessionStreamId = serverSessionStreamId;
    this.connectRequestId = connectRequestId;
    this.options = options;
    this.sessionId = sessionId;
    this.aeronResources = aeronResources;
    this.clientControlPublication =
        aeronResources.publication(
            category,
            clientChannel,
            clientControlStreamId,
            "to send control requests to client",
            sessionId);
  }

  Mono<Void> connect() {
    return Mono.defer(
        () -> {
          Duration retryInterval = Duration.ofMillis(100);
          Duration connectTimeout = options.connectTimeout().plus(options.backpressureTimeout());
          long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

          DefaultMessagePublication publication =
              new DefaultMessagePublication(
                  aeronResources, clientControlPublication, category, 0, 0);

          return sendConnectAck(publication)
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .timeout(connectTimeout)
              .then()
              .doOnSuccess(
                  avoid ->
                      logger.debug(
                          "[{}] Sent {} to {}", category, MessageType.CONNECT_ACK, publication))
              .onErrorResume(
                  throwable -> {
                    String errMessage =
                        String.format(
                            "Failed to send %s, publication %s is not connected",
                            MessageType.CONNECT_ACK, publication);
                    return Mono.error(new RuntimeException(errMessage, throwable));
                  });
        });
  }

  private Mono<Void> sendConnectAck(DefaultMessagePublication publication) {
    return publication.enqueue(
        MessageType.CONNECT_ACK,
        Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId),
        sessionId);
  }

  @Override
  public void dispose() {
    aeronResources.close(clientControlPublication);
  }
}
