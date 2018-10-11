package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

abstract class MessageOperation<T> {
    private static final int STATE_PENDING = 0;

    private static final int STATE_COMPLETED = 1;

    private static final AtomicIntegerFieldUpdater<MessageOperation> STATE = newUpdater(MessageOperation.class, "state");

    private final T message;

    private final MessageOperationOpts<T> opts;

    private final Object affinityKey;

    private final int affinity;

    private final MessagingWorker worker;

    private final MessagingGatewayContext<T> gateway;

    private Future<?> timeoutFuture;

    private SendPressureGuard sendPressure;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int state;

    public MessageOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        boolean threadAffinity
    ) {
        this.message = message;
        this.gateway = gateway;
        this.opts = opts;
        this.affinityKey = affinityKey;

        if (affinityKey == null) {
            // Use artificial affinity.
            affinity = ThreadLocalRandom.current().nextInt();

            if (threadAffinity) {
                worker = gateway.async().workerFor(affinity);
            } else {
                worker = gateway.async().pooledWorker();
            }
        } else {
            // Use key-based affinity.
            affinity = affinityKey.hashCode();

            worker = gateway.async().workerFor(affinity);
        }
    }

    public abstract ClusterNodeId route(PartitionMapper mapper, Optional<FailureInfo> prevFailure) throws LoadBalancerException;

    public abstract OutboundType type();

    public abstract ReplyDecision accept(Throwable error, Response<T> response);

    public abstract CompletableFuture<?> future();

    protected abstract void doReceiveFinal(Response<T> response);

    protected abstract void doFail(Throwable error);

    public void registerTimeout(Future<?> timeoutFuture) {
        this.timeoutFuture = timeoutFuture;
    }

    public void registerSendPressure(SendPressureGuard sendPressure) {
        this.sendPressure = sendPressure;
    }

    public boolean isDone() {
        return state == STATE_COMPLETED;
    }

    public boolean complete(Throwable error, Response<T> response) {
        if (response != null && response.isPartial()) {
            // Do not complete on partial responses.
            doReceivePartial(response);
        } else if (STATE.compareAndSet(this, STATE_PENDING, STATE_COMPLETED)) {
            if (sendPressure != null) {
                sendPressure.onDequeue();
            }

            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }

            if (error == null) {
                doReceiveFinal(response);
            } else {
                doFail(error);
            }

            return true;
        }

        return false;
    }

    public boolean shouldExpireOnTimeout() {
        return true;
    }

    public T message() {
        return message;
    }

    public MessagingGatewayContext<T> gateway() {
        return gateway;
    }

    public MessageOperationOpts<T> opts() {
        return opts;
    }

    public Object affinityKey() {
        return affinityKey;
    }

    public boolean hasAffinity() {
        return affinityKey != null;
    }

    public int affinity() {
        return affinity;
    }

    public MessagingWorker worker() {
        return worker;
    }

    protected void doReceivePartial(Response<T> response) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " can't receive " + response);
    }
}
