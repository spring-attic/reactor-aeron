package reactor.ipc.aeron.server;

import io.aeron.Publication;
import java.util.UUID;
import java.util.concurrent.Callable;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.DefaultMessagePublication;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.RetryTask;
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

  private final HeartbeatSender heartbeatSender;

  private volatile Disposable heartbeatSenderDisposable =
      () -> {
        // no-op
      };

  ServerConnector(
      String category,
      AeronWrapper wrapper,
      String clientChannel,
      int clientControlStreamId,
      long sessionId,
      int serverSessionStreamId,
      UUID connectRequestId,
      AeronOptions options,
      HeartbeatSender heartbeatSender) {
    this.category = category;
    this.serverSessionStreamId = serverSessionStreamId;
    this.connectRequestId = connectRequestId;
    this.options = options;
    this.sessionId = sessionId;
    this.heartbeatSender = heartbeatSender;
    this.clientControlPublication =
        wrapper.addPublication(
            clientChannel, clientControlStreamId, "to send control requests to client", sessionId);
  }

  Mono<Void> connect() {
    return Mono.create(
            (MonoSink<Void> sink) -> {
              int retryMillis = 100;
              int timeoutMillis =
                  options.connectTimeoutMillis() + options.controlBackpressureTimeoutMillis();
              RetryTask retryTask =
                  new RetryTask(
                      Schedulers.single(),
                      retryMillis,
                      timeoutMillis,
                      new SendConnectAckTask(sink),
                      throwable -> {
                        String errMessage =
                            String.format(
                                "Failed to send %s into %s",
                                MessageType.CONNECT_ACK,
                                AeronUtils.format(clientControlPublication));
                        RuntimeException exception = new RuntimeException(errMessage, throwable);
                        sink.error(exception);
                      });
              retryTask.schedule();
            })
        .then(
            Mono.fromRunnable(
                () ->
                    this.heartbeatSenderDisposable =
                        heartbeatSender
                            .scheduleHeartbeats(clientControlPublication, sessionId)
                            .subscribe(
                                ignore -> {
                                  // no-op
                                },
                                th -> {
                                  // no-op
                                })));
  }

  @Override
  public void dispose() {
    heartbeatSenderDisposable.dispose();

    clientControlPublication.close();
  }

  class SendConnectAckTask implements Callable<Boolean> {

    private final MessagePublication publication;

    private final MonoSink<?> sink;

    SendConnectAckTask(MonoSink<?> sink) {
      this.sink = sink;
      this.publication = new DefaultMessagePublication(clientControlPublication, category, 0, 0);
    }

    @Override
    public Boolean call() throws Exception {
      long result =
          publication.publish(
              MessageType.CONNECT_ACK,
              Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId),
              sessionId);
      if (result > 0) {
        logger.debug(
            "[{}] Sent {} to {}",
            category,
            MessageType.CONNECT_ACK,
            category,
            publication.asString());
        sink.success();
        return true;
      } else if (result == Publication.CLOSED) {
        throw new RuntimeException(
            String.format("Publication %s has been closed", publication.asString()));
      }

      return false;
    }
  }
}
