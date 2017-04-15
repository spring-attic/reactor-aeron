package reactor.ipc.aeron.publisher;

import org.reactivestreams.Subscription;
import reactor.core.publisher.MonoSink;

public interface MergePublisherSubscription extends Subscription {

    long getProduced();

    void drainNextPublisher();

    MonoSink<?> getPromise();

}