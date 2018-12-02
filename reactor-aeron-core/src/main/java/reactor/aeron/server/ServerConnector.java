package reactor.aeron.server;

import io.aeron.Publication;
import java.util.UUID;
import java.util.concurrent.Callable;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.DefaultMessagePublication;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.Protocol;
import reactor.aeron.RetryTask;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
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
    return Mono.create(sink -> createConnectRetryTask(sink).schedule());
  }

  private RetryTask createConnectRetryTask(MonoSink<Void> sink) {
    return new RetryTask(
        Schedulers.single(),
        100,
        options.connectTimeout().toMillis() + options.backpressureTimeout().toMillis(),
        new SendConnectAckTask(sink),
        throwable -> {
          String errMessage =
              String.format(
                  "Failed to send %s into %s",
                  MessageType.CONNECT_ACK, AeronUtils.format(clientControlPublication));
          RuntimeException exception = new RuntimeException(errMessage, throwable);
          sink.error(exception);
        });
  }

  @Override
  public void dispose() {
    aeronResources.close(clientControlPublication);
  }

  class SendConnectAckTask implements Callable<Boolean> {

    private final MessagePublication publication;

    private final MonoSink<Void> sink;

    SendConnectAckTask(MonoSink<Void> sink) {
      this.sink = sink;
      this.publication =
          new DefaultMessagePublication(aeronResources, clientControlPublication, category, 0, 0);
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
