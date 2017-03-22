package io.hekate.messaging.internal;

import io.hekate.util.format.ToString;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class MessageContext<T> {
    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<MessageContext> COMPLETED = newUpdater(MessageContext.class, "completed");

    private final T message;

    private final int affinity;

    private final Object affinityKey;

    private final AffinityWorker worker;

    private volatile int completed;

    public MessageContext(T message, MessageContext<T> src) {
        this(message, src.getAffinity(), src.getAffinityKey(), src.getWorker());
    }

    public MessageContext(T message, int affinity, Object affinityKey, AffinityWorker worker) {
        this.message = message;
        this.affinity = affinity;
        this.affinityKey = affinityKey;
        this.worker = worker;
    }

    public boolean complete() {
        return COMPLETED.compareAndSet(this, 0, 1);
    }

    public boolean isCompleted() {
        return completed == 1;
    }

    public T getMessage() {
        return message;
    }

    public int getAffinity() {
        return affinity;
    }

    public Object getAffinityKey() {
        return affinityKey;
    }

    public AffinityWorker getWorker() {
        return worker;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
