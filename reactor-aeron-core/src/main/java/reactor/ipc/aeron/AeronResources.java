package reactor.ipc.aeron;

import io.aeron.Aeron;
import io.aeron.driver.AeronWrapper;
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

  public Pooler pooler() {
    return pooler;
  }

  public AeronWrapper aeronWrapper() {
    return aeronWrapper;
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

  @Override
  public String toString() {
    return "AeronResources[" + name + "]";
  }
}
