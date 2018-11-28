package reactor.aeron;

import io.aeron.Publication;
import reactor.core.publisher.Mono;

public interface SenderLoop {

  Mono<Void> register(MessagePublication messagePublication);

  Mono<Void> unregister(MessagePublication messagePublication);

  Mono<Void> close(Publication publication);
}
