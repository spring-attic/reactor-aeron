package reactor.ipc.aeron;

import org.reactivestreams.Subscription;

public interface PollerSubscriber {

  void onSubscribe(Subscription subscription);
}
