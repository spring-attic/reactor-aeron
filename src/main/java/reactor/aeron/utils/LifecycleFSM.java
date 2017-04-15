package reactor.aeron.utils;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 *
 *
 * NOT_STARTED -> STARTED -> TERMINATING -> TERMINATED
 *     |                                        ^
 *     --------------------->--------------------
 *
 * @author Anatoly Kadyshev
 */
public final class LifecycleFSM {

    enum State {
        NOT_STARTED,
        STARTED,
        TERMINATING,
        TERMINATED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    public boolean setStarted() {
        return state.compareAndSet(State.NOT_STARTED, State.STARTED);
    }

    public boolean isStarted() {
        return state.get() == State.STARTED;
    }

    /**
     * @return true if state changed into a terminal state
     */
    public boolean terminate() {
        boolean success;
        do {
            if (state.get() == State.TERMINATING || state.get() == State.TERMINATED) {
                return false;
            }

            success =
                    state.compareAndSet(State.NOT_STARTED, State.TERMINATED) ||
                    state.compareAndSet(State.STARTED, State.TERMINATING);
        } while(!success);
        return true;
    }

    public void setTerminated() {
        state.compareAndSet(State.TERMINATING, State.TERMINATED);
    }

    public boolean isTerminated() {
        return state.get() == State.TERMINATED;
    }

}
