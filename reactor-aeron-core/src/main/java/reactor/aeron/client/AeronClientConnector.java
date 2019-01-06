package reactor.aeron.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronConnection;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageSubscription;
import reactor.core.publisher.Mono;

/**
 * Full-duplex aeron client connector. Schematically can be described as:
 *
 * <pre>
 * Client
 * serverPort->outbound->Pub(endpoint, sessionId)
 * serverControlPort->inbound->MDC(sessionId)->Sub(control-endpoint, sessionId)</pre>
 */
public final class AeronClientConnector {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientConnector.class);

  private final AeronOptions options;
  private final AeronResources resources;

  AeronClientConnector(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();
  }

  /**
   * Creates and setting up {@link Connection} object and everyting around it.
   *
   * @return mono result
   */
  public Mono<Connection> start() {
    return Mono.defer(
        () -> {
          // outbound->Pub(endpoint, sessionId)
          String outboundChannel = options.outboundUri().toString();

          return resources
              .publication(outboundChannel, options)
              // TODO here problem possible since sessionId is not globally unique; need to
              //  retry few times if connection wasn't succeeeded
              .flatMap(MessagePublication::ensureConnected)
              .flatMap(
                  publication -> {
                    // inbound->MDC(sessionId)->Sub(control-endpoint, sessionId)
                    int sessionId = publication.sessionId();
                    String inboundChannel = options.inboundUri().sessionId(sessionId).toString();

                    DefaultAeronInbound inbound = new DefaultAeronInbound();
                    DefaultAeronOutbound outbound = new DefaultAeronOutbound(publication);

                    Mono<MessageSubscription> subscriptionMono =
                        resources
                            .subscription(
                                inboundChannel,
                                inbound,
                                image -> {
                                  // TODO image avaliablae -- good , probalbly log it and that's it
                                },
                                image -> {
                                  // TODO image unavalablei -- dispose entire connection (along with
                                  //  inbound/outbound/ and publication)
                                })
                            .doOnError(
                                th -> {
                                  // TODO dispose eveytihngf craetedf  previosusly
                                });

                    return subscriptionMono.map(
                        subscription -> new DefaultAeronConnection(sessionId, inbound, outbound));
                  });
        });
  }
}
