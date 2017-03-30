package io.hekate.messaging.internal;

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class RequestHandle<T> {
    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<RequestHandle> UNREGISTERED = newUpdater(RequestHandle.class, "unregistered");

    private final Integer id;

    private final int epoch;

    private final MessageContext<T> ctx;

    @ToStringIgnore
    private final InternalRequestCallback<T> callback;

    @ToStringIgnore
    private final RequestRegistry<T> registry;

    @ToStringIgnore
    @SuppressWarnings("unused")
    private volatile int unregistered;

    public RequestHandle(Integer id, RequestRegistry<T> registry, MessageContext<T> ctx, int epoch, InternalRequestCallback<T> callback) {
        this.id = id;
        this.registry = registry;
        this.ctx = ctx;
        this.epoch = epoch;
        this.callback = callback;
    }

    public Integer getId() {
        return id;
    }

    public AffinityWorker getWorker() {
        return ctx.getWorker();
    }

    public T getMessage() {
        return ctx.getMessage();
    }

    public int getEpoch() {
        return epoch;
    }

    public InternalRequestCallback<T> getCallback() {
        return callback;
    }

    public MessageContext<T> getContext() {
        return ctx;
    }

    public boolean isRegistered() {
        return unregistered == 0;
    }

    public boolean unregister() {
        if (UNREGISTERED.compareAndSet(this, 0, 1)) {
            registry.unregister(id);

            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
