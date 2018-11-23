package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryResponsePolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.ResponsePart;

class RequestOperation<T> extends UnicastOperation<T> {
    private final RequestFuture<T> future = new RequestFuture<>();

    private final RetryResponsePolicy<T> retryRsp;

    public RequestOperation(
        T message,
        Object affinityKey,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryResponsePolicy<T> retryRsp,
        RetryCondition retryCondition,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts
    ) {
        super(message, affinityKey, maxAttempts, retryErr, retryCondition, retryCallback, retryRoute, gateway, opts, false);

        this.retryRsp = retryRsp;
    }

    @Override
    public OutboundType type() {
        return OutboundType.REQUEST;
    }

    @Override
    public RequestFuture<T> future() {
        return future;
    }

    @Override
    public boolean shouldRetry(ResponsePart<T> response) {
        return retryRsp != null && retryRsp.shouldRetry(response);
    }

    @Override
    protected void doReceiveFinal(ResponsePart<T> response) {
        future.complete(response);
    }

    @Override
    protected void doFail(Throwable error) {
        future.completeExceptionally(error);
    }
}
