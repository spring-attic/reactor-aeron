package reactor.ipc.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.AeronWrapper;
import io.aeron.driver.AeronWriteSequencer;
import reactor.core.Disposable;

public class AeronResources implements Disposable, AutoCloseable {
  private final String name;
  private final AeronWrapper aeronWrapper;
  private final Pooler pooler;

  private volatile boolean isRunning = true;

  public AeronResources(String name) {
    this(name, null);
  }

  /**
   * Creates aeron resources.
   *
   * @param name name
   * @param aeron aeron
   */
  public AeronResources(String name, Aeron aeron) {
    this.name = name;
    this.aeronWrapper = new AeronWrapper(name, aeron);
    this.pooler = new Pooler(name);
    pooler.initialise();
  }

  /**
   * Adds publication. Also see {@link #release(Publication)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @return publication
   */
  public Publication publication(String channel, int streamId, String purpose, long sessionId) {
    return aeronWrapper.addPublication(channel, streamId, purpose, sessionId);
  }

  /**
   * Adds control subscription and register it in the {@link Pooler}. Also see {@link
   * #release(Subscription)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @param controlMessageSubscriber control message subscriber
   * @return control subscription
   */
  public Subscription controlSubscription(
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      ControlMessageSubscriber controlMessageSubscriber) {
    Subscription subscription = aeronWrapper.addSubscription(channel, streamId, purpose, sessionId);
    pooler.addControlSubscription(subscription, controlMessageSubscriber);
    return subscription;
  }

  /**
   * Adds data subscription and register it in the {@link Pooler}. Also see {@link
   * #release(Subscription)}}.
   *
   * @param channel channel
   * @param streamId stream id
   * @param purpose purpose
   * @param sessionId session id
   * @param dataMessageSubscriber data message subscriber
   * @return control subscription
   */
  public Subscription dataSubscription(
      String channel,
      int streamId,
      String purpose,
      long sessionId,
      DataMessageSubscriber dataMessageSubscriber) {
    Subscription subscription = aeronWrapper.addSubscription(channel, streamId, purpose, sessionId);
    pooler.addDataSubscription(subscription, dataMessageSubscriber);
    return subscription;
  }

  /**
   * Releases the given subscription.
   *
   * @param subscription subscription
   */
  public void release(Subscription subscription) {
    if (subscription != null) {
      pooler.removeSubscription(subscription);
      subscription.close();
    }
  }

  /**
   * Releases the given publication.
   *
   * @param publication publication
   */
  public void release(Publication publication) {
    if (publication != null) {
      publication.close();
    }
  }

  /**
   * Creates new AeronWriteSequencer.
   *
   * @param category catergory
   * @param publication publication (see {@link #publication(String, int, String, long)}
   * @param sessionId session id
   * @return new write sequencer
   */
  public AeronWriteSequencer newWriteSequencer(
      String category, MessagePublication publication, long sessionId) {
    return aeronWrapper.newWriteSequencer(category, publication, sessionId);
  }

  @Override
  public void close() {
    dispose();
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      isRunning = false;
      pooler
          .shutdown()
          .subscribe(
              null,
              th -> {
                /* todo */
              });
      aeronWrapper.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return !isRunning;
  }
}
