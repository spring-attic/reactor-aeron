package reactor.aeron.client;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.DefaultMessagePublication;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.Protocol;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

/** Client connector. */
final class ClientConnector implements Disposable {

  private static final Logger logger = Loggers.getLogger(ClientConnector.class);

  private static final TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator();

  private final String category;

  private final AeronClientOptions options;

  private final UUID connectRequestId;

  private final ClientControlMessageSubscriber controlMessageSubscriber;

  private final int clientControlStreamId;

  private final int clientSessionStreamId;

  private final io.aeron.Publication serverControlPublication;

  private final AeronResources aeronResources;

  private volatile long sessionId;

  ClientConnector(
      String category,
      AeronResources aeronResources,
      AeronClientOptions options,
      ClientControlMessageSubscriber controlMessageSubscriber,
      int clientControlStreamId,
      int clientSessionStreamId) {
    this.category = category;
    this.aeronResources = aeronResources;
    this.options = options;
    this.controlMessageSubscriber = controlMessageSubscriber;
    this.clientControlStreamId = clientControlStreamId;
    this.clientSessionStreamId = clientSessionStreamId;
    this.connectRequestId = uuidGenerator.generate();
    this.serverControlPublication =
        aeronResources.publication(
            category,
            options.serverChannel(),
            options.serverStreamId(),
            "to send control requests to server",
            0);
  }

  Mono<ClientControlMessageSubscriber.ConnectAckResponse> connect() {
    ClientControlMessageSubscriber.ConnectAckSubscription connectAckSubscription =
        controlMessageSubscriber.subscribeForConnectAck(connectRequestId);

    return sendConnectRequest()
        .then(
            connectAckSubscription
                .connectAck()
                .timeout(options.ackTimeout())
                .onErrorMap(
                    TimeoutException.class,
                    th -> {
                      throw new RuntimeException(
                          String.format(
                              "Failed to receive %s during %d millis",
                              MessageType.CONNECT_ACK, options.ackTimeout().toMillis()),
                          th);
                    }))
        .doOnSuccess(
            response -> {
              this.sessionId = response.sessionId;

              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] Successfully connected to server at {}, sessionId: {}",
                    category,
                    AeronUtils.format(serverControlPublication),
                    sessionId);
              }
            })
        .doOnTerminate(connectAckSubscription::dispose)
        .onErrorMap(
            th -> {
              throw new RuntimeException(
                  String.format(
                      "Failed to connect to server at %s",
                      AeronUtils.format(serverControlPublication)));
            });
  }

  private Mono<Void> sendConnectRequest() {
    ByteBuffer buffer =
        Protocol.createConnectBody(
            connectRequestId,
            options.clientChannel(),
            clientControlStreamId,
            clientSessionStreamId);
    return Mono.fromRunnable(this::logConnect).then(send(buffer, MessageType.CONNECT));
  }

  private void logConnect() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Connecting to server at {}", category, AeronUtils.format(serverControlPublication));
    }
  }

  private Mono<Void> sendDisconnectRequest() {
    ByteBuffer buffer = Protocol.createDisconnectBody(sessionId);
    return Mono.fromRunnable(this::logDisconnect).then(send(buffer, MessageType.COMPLETE));
  }

  private void logDisconnect() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Disconnecting from server at {}",
          category,
          AeronUtils.format(serverControlPublication));
    }
  }

  private Mono<Void> send(ByteBuffer buffer, MessageType msgType) {
    return Mono.create(
        sink -> {
          Exception cause = null;
          try {
            MessagePublication messagePublication =
                new DefaultMessagePublication(
                    aeronResources,
                    serverControlPublication,
                    category,
                    aeronResources.mtuLength(),
                    options.connectTimeoutMillis(),
                    options.controlBackpressureTimeoutMillis());

            long result = messagePublication.enqueue(msgType, buffer, sessionId);
            if (result > 0) {
              logger.debug(
                  "[{}] Sent {} to {}", category, msgType, messagePublication.toString());
              sink.success();
              return;
            }
          } catch (Exception ex) {
            cause = ex;
          }
          sink.error(new RuntimeException("Failed to send message of type: " + msgType, cause));
        });
  }

  @Override
  public void dispose() {
    sendDisconnectRequest()
        .subscribe(
            null,
            th -> {
              // no-op
            });
    aeronResources.close(serverControlPublication);
  }
}
