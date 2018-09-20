package reactor.ipc.aeron;

import org.reactivestreams.Subscription;

public interface PoolerSubscriber {

  void onSubscribe(Subscription subscription);
}
