package reactor.ipc.aeron;

import org.reactivestreams.Subscription;

/** @author Anatoly Kadyshev */
public interface PoolerSubscriber {

  void onSubscribe(Subscription subscription);
}
