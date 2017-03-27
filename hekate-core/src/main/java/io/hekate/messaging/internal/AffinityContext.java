package io.hekate.messaging.internal;

class AffinityContext<T> extends MessageContext<T> {
    private final int affinity;

    private final Object affinityKey;

    public AffinityContext(T message, int affinity, Object affinityKey, AffinityWorker worker, boolean hasTimeout) {
        super(message, worker, hasTimeout);

        this.affinity = affinity;
        this.affinityKey = affinityKey;
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
}
