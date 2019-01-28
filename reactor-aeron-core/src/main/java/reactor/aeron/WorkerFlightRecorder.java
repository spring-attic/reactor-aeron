package reactor.aeron;

class WorkerFlightRecorder {

  /**
   * JMX MBean exposer class for event loop worker thread (see for details {@link AeronEventLoop}).
   * Contains various runtime stats.
   */
  public interface WorkerMBean {

    /**
     * Returns number of ticks per last second.
     *
     * @return number of ticks per last seconds
     */
    long getTicks();

    /**
     * Returns number of work done (counted both outbound and inbound) per last second.
     *
     * @return number of work done per last second.
     */
    long getWorkCount();

    /**
     * Returns number of how many times event loop was idling without progress done.
     *
     * @return number of times being idle
     */
    long getIdleCount();


    double getOutboundRate();

    double getInboundRate();


  }
}
