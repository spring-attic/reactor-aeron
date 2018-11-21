package reactor.ipc.aeron.client;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import io.aeron.Publication;
import io.aeron.driver.AeronResources;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.DefaultMessagePublication;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Protocol;
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

  private final Publication serverControlPublication;

  private final HeartbeatSender heartbeatSender;

  private final AeronResources aeronResources;

  private volatile long sessionId;

  private volatile Disposable heartbeatSenderDisposable =
      () -> {
        // no-op
      };

  ClientConnector(
      String category,
      AeronResources aeronResources,
      AeronClientOptions options,
      ClientControlMessageSubscriber controlMessageSubscriber,
      HeartbeatSender heartbeatSender,
      int clientControlStreamId,
      int clientSessionStreamId) {
    this.category = category;
    this.aeronResources = aeronResources;
    this.options = options;
    this.controlMessageSubscriber = controlMessageSubscriber;
    this.clientControlStreamId = clientControlStreamId;
    this.clientSessionStreamId = clientSessionStreamId;
    this.heartbeatSender = heartbeatSender;
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

              heartbeatSenderDisposable =
                  heartbeatSender
                      .scheduleHeartbeats(serverControlPublication, sessionId)
                      .subscribe(
                          avoid -> {
                            // no-op
                          },
                          th -> {
                            // no-op
                          });

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

  private Mono<Void> send(ByteBuffer buffer, MessageType messageType) {
    return Mono.create(
        sink -> {
          Exception cause = null;
          try {
            MessagePublication messagePublication =
                new DefaultMessagePublication(
                    serverControlPublication,
                    category,
                    options.connectTimeoutMillis(),
                    options.controlBackpressureTimeoutMillis());

            long result = messagePublication.publish(messageType, buffer, sessionId);
            if (result > 0) {
              logger.debug(
                  "[{}] Sent {} to {}", category, messageType, messagePublication.asString());
              sink.success();
              return;
            }
          } catch (Exception ex) {
            cause = ex;
          }
          sink.error(new RuntimeException("Failed to send message of type: " + messageType, cause));
        });
  }

  @Override
  public void dispose() {
    sendDisconnectRequest()
        .subscribe(
            avoid -> {
              // no-op
            },
            th -> {
              // no-op
            });

    heartbeatSenderDisposable.dispose();

    aeronResources.close(serverControlPublication);
  }
}
