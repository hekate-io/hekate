package io.hekate.messaging.internal;

import io.hekate.util.format.ToString;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class MessageContext<T> {
    interface TimeoutListener {
        void onTimeout();
    }

    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<MessageContext> COMPLETED = newUpdater(MessageContext.class, "completed");

    private final T message;

    private final AffinityWorker worker;

    private final boolean hasTimeout;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int completed;

    private TimeoutListener timeoutListener;

    private Future<?> timeoutFuture;

    public MessageContext(T message, AffinityWorker worker, boolean hasTimeout) {
        this.message = message;
        this.worker = worker;
        this.hasTimeout = hasTimeout;
    }

    public boolean complete() {
        boolean completed = doComplete();

        if (completed && timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }

        return completed;
    }

    public boolean completeOnTimeout() {
        boolean completed = complete();

        if (completed) {
            if (timeoutListener != null) {
                timeoutListener.onTimeout();
            }
        }

        return completed;
    }

    public boolean isCompleted() {
        return completed == 1;
    }

    public T getMessage() {
        return message;
    }

    public AffinityWorker getWorker() {
        return worker;
    }

    public void setTimeoutListener(TimeoutListener timeoutListener) {
        assert hasTimeout : "Timeout listener can be set only for time-limited contexts.";

        this.timeoutListener = timeoutListener;
    }

    public void setTimeoutFuture(Future<?> timeoutFuture) {
        this.timeoutFuture = timeoutFuture;
    }

    private boolean doComplete() {
        return COMPLETED.compareAndSet(this, 0, 1);
    }

    boolean hasTimeout() {
        return hasTimeout;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
