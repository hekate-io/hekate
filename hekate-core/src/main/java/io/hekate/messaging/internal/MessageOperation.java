package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
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

    private final RetryErrorPolicy retryErr;

    private final RetryCondition retryCondition;

    private final RetryBackoffPolicy retryBackoff;

    private final RetryCallback retryCallback;

    private final RetryRoutingPolicy retryRoute;

    private final int maxAttempts;

    private final long timeout;

    private final MessagingWorker worker;

    private final MessagingGatewayContext<T> gateway;

    private Future<?> timeoutFuture;

    private SendPressureGuard sendPressure;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int state;

    public MessageOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        boolean threadAffinity
    ) {
        this.message = message;
        this.affinityKey = affinityKey;
        this.maxAttempts = maxAttempts;
        this.timeout = timeout;
        this.gateway = gateway;
        this.retryErr = retryErr;
        this.retryCondition = retryCondition;
        this.retryBackoff = retryBackoff;
        this.retryCallback = retryCallback;
        this.retryRoute = retryRoute;
        this.opts = opts;

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

    public abstract ClusterNodeId route(PartitionMapper mapper, Optional<FailedAttempt> prevFailure) throws LoadBalancerException;

    public abstract OutboundType type();

    public abstract boolean shouldRetry(ResponsePart<T> response);

    public abstract CompletableFuture<?> future();

    protected abstract void doReceiveFinal(ResponsePart<T> response);

    protected abstract void doFail(Throwable error);

    public long timeout() {
        return timeout;
    }

    public boolean hasTimeout() {
        return timeout > 0;
    }

    public void registerTimeout(Future<?> timeoutFuture) {
        this.timeoutFuture = timeoutFuture;
    }

    public void registerSendPressure(SendPressureGuard sendPressure) {
        this.sendPressure = sendPressure;
    }

    public boolean isDone() {
        return state == STATE_COMPLETED;
    }

    public boolean canRetry() {
        return retryCondition == null || retryCondition.shouldRetry();
    }

    public void onRetry(FailedAttempt failure) {
        if (retryCallback != null) {
            retryCallback.onRetry(failure);
        }
    }

    public boolean complete(Throwable error, ResponsePart<T> response) {
        if (response != null && !response.isLastPart()) {
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

    public RetryErrorPolicy retryErrorPolicy() {
        return retryErr;
    }

    public RetryRoutingPolicy retryRoute() {
        return retryRoute;
    }

    public int maxAttempts() {
        return maxAttempts;
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

    public RetryBackoffPolicy retryBackoff() {
        return retryBackoff;
    }

    protected void doReceivePartial(ResponsePart<T> response) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " can't receive " + response);
    }
}
