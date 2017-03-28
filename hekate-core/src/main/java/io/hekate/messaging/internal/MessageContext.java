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

    protected final int affinity;

    protected final Object affinityKey;

    private final T message;

    private final AffinityWorker worker;

    private final boolean hasTimeout;

    private TimeoutListener timeoutListener;

    private Future<?> timeoutFuture;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int completed;

    public MessageContext(T message, int affinity, Object affinityKey, AffinityWorker worker, boolean hasTimeout) {
        this.message = message;
        this.worker = worker;
        this.hasTimeout = hasTimeout;
        this.affinityKey = affinityKey;
        this.affinity = affinity;
    }

    public boolean isStrictAffinity() {
        return affinity >= 0;
    }

    public int getAffinity() {
        return affinity;
    }

    public Object getAffinityKey() {
        return affinityKey;
    }

    public T getMessage() {
        return message;
    }

    public AffinityWorker getWorker() {
        return worker;
    }

    public boolean isCompleted() {
        return completed == 1;
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
