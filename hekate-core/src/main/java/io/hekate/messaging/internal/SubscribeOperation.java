package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryResponsePolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.messaging.unicast.ResponsePart;
import io.hekate.messaging.unicast.SubscribeCallback;
import io.hekate.messaging.unicast.SubscribeFuture;

class SubscribeOperation<T> extends UnicastOperation<T> {
    private final SubscribeFuture<T> future = new SubscribeFuture<>();

    private final SubscribeCallback<T> callback;

    private final RetryResponsePolicy<T> retryRsp;

    private volatile boolean active;

    public SubscribeOperation(
        T message,
        Object affinityKey,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryResponsePolicy<T> retryRsp,
        RetryCondition retryCondition,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        SubscribeCallback<T> callback
    ) {
        super(message, affinityKey, maxAttempts, retryErr, retryCondition, retryCallback, retryRoute, gateway, opts, true);

        this.callback = callback;
        this.retryRsp = retryRsp;
    }

    @Override
    public OutboundType type() {
        return OutboundType.SUBSCRIBE;
    }

    @Override
    public SubscribeFuture<T> future() {
        return future;
    }

    @Override
    public boolean shouldRetry(ResponsePart<T> response) {
        return retryRsp != null
            && !isPartial(response)
            && retryRsp.shouldRetry(response);
    }

    @Override
    public boolean shouldExpireOnTimeout() {
        if (active) {
            // Reset the flag so that we could detect inactivity upon the next invocation of this method.
            active = false;

            return false;
        } else {
            // No activity between this invocation and the previous one.
            return true;
        }
    }

    @Override
    protected void doReceivePartial(ResponsePart<T> response) {
        if (!active) {
            active = true;
        }

        callback.onComplete(null, response);
    }

    @Override
    protected void doReceiveFinal(ResponsePart<T> response) {
        try {
            callback.onComplete(null, response);
        } finally {
            future.complete(response);
        }
    }

    @Override
    protected void doFail(Throwable error) {
        try {
            callback.onComplete(error, null);
        } finally {
            future.completeExceptionally(error);
        }
    }

    private static boolean isPartial(ResponsePart<?> response) {
        return response != null && !response.isLastPart();
    }
}
