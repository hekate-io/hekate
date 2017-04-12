package io.hekate.messaging.internal;

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class MessageContext<T> {
    interface TimeoutListener {
        void onTimeout();
    }

    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<MessageContext> COMPLETED = newUpdater(MessageContext.class, "completed");

    private final int affinity;

    private final Object affinityKey;

    private final boolean stream;

    private final T message;

    @ToStringIgnore
    private final MessagingWorker worker;

    @ToStringIgnore
    private final MessagingOpts<T> opts;

    @ToStringIgnore
    private TimeoutListener timeoutListener;

    @ToStringIgnore
    private volatile Future<?> timeoutFuture;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int completed;

    public MessageContext(T message, int affinity, Object affinityKey, MessagingWorker worker, MessagingOpts<T> opts, boolean stream) {
        assert message != null : "Message is null.";
        assert worker != null : "Worker is null.";
        assert opts != null : "Messaging options are null.";

        this.message = message;
        this.worker = worker;
        this.opts = opts;
        this.affinityKey = affinityKey;
        this.affinity = affinity;
        this.stream = stream;
    }

    public boolean hasAffinity() {
        return affinity >= 0;
    }

    public int affinity() {
        return affinity;
    }

    public Object affinityKey() {
        return affinityKey;
    }

    public boolean isStream() {
        return stream;
    }

    public T message() {
        return message;
    }

    public MessagingWorker worker() {
        return worker;
    }

    public MessagingOpts<T> opts() {
        return opts;
    }

    public boolean isCompleted() {
        return completed == 1;
    }

    public boolean complete() {
        boolean completed = doComplete();

        if (completed) {
            Future<?> localFuture = this.timeoutFuture;

            if (localFuture != null) {
                localFuture.cancel(false);
            }
        }

        return completed;
    }

    public boolean completeOnTimeout() {
        boolean completed = doComplete();

        if (completed) {
            if (timeoutListener != null) {
                timeoutListener.onTimeout();
            }
        }

        return completed;
    }

    public void setTimeoutListener(TimeoutListener timeoutListener) {
        assert opts.hasTimeout() : "Timeout listener can be set only for time-limited contexts.";

        this.timeoutListener = timeoutListener;
    }

    public void setTimeoutFuture(Future<?> timeoutFuture) {
        Future<?> oldFuture = this.timeoutFuture;

        if (oldFuture != null) {
            oldFuture.cancel(false);
        }

        this.timeoutFuture = timeoutFuture;
    }

    private boolean doComplete() {
        return COMPLETED.compareAndSet(this, 0, 1);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
