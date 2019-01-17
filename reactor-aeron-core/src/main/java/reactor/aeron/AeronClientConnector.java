package reactor.aeron;

import io.aeron.Image;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron client connector. Schematically can be described as:
 *
 * <pre>
 * Client
 * serverPort->outbound->Pub(endpoint, sessionId)
 * serverControlPort->inbound->MDC(sessionId)->Sub(control-endpoint, sessionId)</pre>
 */
final class AeronClientConnector {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientConnector.class);

  /** The stream ID that the server and client use for messages. */
  private static final int STREAM_ID = 0xcafe0000;

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super AeronConnection, ? extends Publisher<Void>> handler;

  AeronClientConnector(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();
    this.handler = options.handler();
  }

  /**
   * Creates and setting up {@link AeronConnection} object and everyting around it.
   *
   * @return mono result
   */
  Mono<AeronConnection> start() {
    return Mono.defer(
        () -> {
          // outbound->Pub(endpoint, sessionId)
          String outboundChannel = options.outboundUri().asString();

          return resources
              .publication(outboundChannel, STREAM_ID, options)
              // TODO here problem possible since sessionId is not globally unique; need to
              //  retry few times if connection wasn't succeeeded
              .flatMap(MessagePublication::ensureConnected)
              .flatMap(
                  publication -> {
                    // inbound->MDC(sessionId)->Sub(control-endpoint, sessionId)
                    int sessionId = publication.sessionId();
                    String inboundChannel =
                        options.inboundUri().with(b -> b.sessionId(sessionId)).asString();
                    logger.debug(
                        "{}: creating client connection: {}",
                        Integer.toHexString(sessionId),
                        inboundChannel);

                    // setup cleanup hook to use it onwards
                    MonoProcessor<Void> disposeHook = MonoProcessor.create();
                    // setup image avaiable hook
                    MonoProcessor<Image> inboundAvailable = MonoProcessor.create();

                    return resources
                        .subscription(
                            inboundChannel,
                            STREAM_ID,
                            image -> {
                              logger.debug(
                                  "{}: created client inbound", Integer.toHexString(sessionId));
                              inboundAvailable.onNext(image);
                            },
                            image -> {
                              logger.debug(
                                  "{}: client inbound became unavaliable",
                                  Integer.toHexString(sessionId));
                              disposeHook.onComplete();
                            })
                        .doOnError(
                            th -> {
                              logger.warn(
                                  "{}: failed to create client inbound, cause: {}",
                                  Integer.toHexString(sessionId),
                                  th.toString());
                              // dispose outbound resource
                              publication.dispose();
                            })
                        .flatMap(
                            subscription ->
                                inboundAvailable.flatMap(
                                    image ->
                                        newConnection(
                                            sessionId,
                                            image,
                                            publication,
                                            subscription,
                                            disposeHook)))
                        .doOnSuccess(
                            connection ->
                                logger.debug(
                                    "{}: created client connection: {}",
                                    Integer.toHexString(sessionId),
                                    inboundChannel));
                  });
        });
  }

  private Mono<AeronConnection> newConnection(
      int sessionId,
      Image image,
      MessagePublication publication,
      MessageSubscription subscription,
      MonoProcessor<Void> disposeHook) {

    return resources
        .inbound(image, subscription)
        .doOnError(
            ex -> {
              subscription.dispose();
              publication.dispose();
            })
        .flatMap(
            inbound -> {
              DefaultAeronOutbound outbound = new DefaultAeronOutbound(publication);

              DuplexAeronConnection connection =
                  new DuplexAeronConnection(sessionId, inbound, outbound, disposeHook);

              return connection.start(handler).doOnError(ex -> connection.dispose());
            });
  }
}
