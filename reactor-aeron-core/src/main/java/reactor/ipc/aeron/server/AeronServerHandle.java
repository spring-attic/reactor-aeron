package reactor.ipc.aeron.server;

import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.OnDisposable;

class AeronServerHandle extends AeronServerOperator {

  private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>>
      handler;

  AeronServerHandle(
      AeronServer aeronServer,
      BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> handler) {
    super(aeronServer);
    this.handler = handler;
  }

  @Override
  public Mono<? extends OnDisposable> bind(AeronOptions options) {
    return super.bind(options)
        .cast(ServerHandler.class)
        .doOnSuccess(
            serverHandler ->
                serverHandler
                    .connections()
                    .subscribe(
                        connection -> {
                          handler
                              .apply(connection.inbound(), connection.outbound())
                              .subscribe(connection.disposeSubscriber());
                        }));
  }
}
