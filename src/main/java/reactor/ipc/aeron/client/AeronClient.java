package reactor.ipc.aeron.client;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronConnector;
import reactor.ipc.aeron.AeronHelper;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.RequestType;
import reactor.ipc.aeron.UUIDUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronClient implements AeronConnector {

    private final AeronClientOptions options;

    private final String name;

    private final AtomicInteger nextClientStreamId = new AtomicInteger(0);

    private AeronClient(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        AeronClientOptions options = new AeronClientOptions();
        optionsConfigurer.accept(options);
        this.options = options;
        this.name = name == null ? "client" : name;
    }

    public static AeronClient create(String name, Consumer<AeronClientOptions> optionsConfigurer) {
        return new AeronClient(name, optionsConfigurer);
    }

    public static AeronClient create(String name) {
        return create(name, options -> {});
    }

    public static AeronClient create() {
        return create(null);
    }

    @Override
    public Mono<? extends Disposable> newHandler(
            BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler) {
        ClientHandler handler = new ClientHandler(ioHandler, name, options, nextClientStreamId.incrementAndGet());
        return handler.start();
    }

    static class ClientHandler implements Disposable {

        private final BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler;

        private final AeronClientOptions options;

        private final String category;

        private final AeronHelper aeronHelper;

        private final UUID sessionId;

        private final AeronClientInbound inbound;

        private final AeronOutbound outbound;

        private final Connector connector;

        ClientHandler(BiFunction<? super AeronInbound, ? super AeronOutbound, ? extends Publisher<Void>> ioHandler,
                      String name,
                      AeronClientOptions options,
                      int clientStreamId) {
            this.ioHandler = ioHandler;
            this.options = options;
            this.category = name;
            this.aeronHelper = new AeronHelper(category, options);
            this.sessionId = UUIDUtils.create();

            this.outbound = new AeronOutbound(category, aeronHelper,
                    this.options.serverChannel(), this.options.serverStreamId(), sessionId, options);

            this.inbound = new AeronClientInbound(aeronHelper,
                    this.options.clientChannel(), clientStreamId, sessionId, name);

            this.connector = new Connector(category, aeronHelper,
                    this.options.serverChannel(), this.options.serverStreamId(),
                    this.options.clientChannel(), clientStreamId,
                    sessionId);
        }

        Mono<Disposable> start() {
                return connector.connect(options.connectTimeoutMillis())
                        .doOnSuccess(avoid ->
                                Mono.from(ioHandler.apply(inbound, outbound)).subscribe(ignore -> dispose()))
                        .doOnTerminate((avoid, th) -> dispose())
                        .then(Mono.just(this));
        }

        @Override
        public void dispose() {
            inbound.shutdown();
            aeronHelper.shutdown();
        }
    }

    static class Connector {

        private final Publication publication;

        private final Logger logger;

        private String clientChannel;

        private int clientStreamId;

        private final UUID sessionId;

        Connector(String category,
                  AeronHelper aeronHelper,
                  String serverChannel, int serverStreamId,
                  String clientChannel, int clientStreamId, UUID sessionId) {
            this.clientChannel = clientChannel;
            this.clientStreamId = clientStreamId;
            this.sessionId = sessionId;
            this.publication = aeronHelper.addPublication(serverChannel, serverStreamId, "sending requests", sessionId);
            this.logger = Loggers.getLogger(AeronClient.class + "." + category);
        }

        Mono<Void> connect(int timeoutMillis) {
            return Mono.create(sink -> {
                IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
                ByteBuffer buffer = AeronUtils.createConnectBody(clientChannel, clientStreamId);
                long result = 0;
                Exception cause = null;
                logger.debug("Connecting to server at: {}", AeronUtils.format(publication));
                try {
                    result = AeronUtils.publish(logger, publication,
                            RequestType.CONNECT, buffer, idleStrategy, sessionId, timeoutMillis, timeoutMillis);
                } catch (Exception ex) {
                    cause = ex;
                }
                if (result > 0) {
                    sink.success();
                } else {
                    String message = "Failed to connect to server";
                    sink.error(cause == null ? new Exception(message): new Exception(message, cause));
                }
            });
        }
    }

}
