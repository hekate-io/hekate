package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.RequestRetryCondition;
import io.hekate.messaging.unicast.ResponsePart;
import io.hekate.messaging.unicast.RetryDecision;

class RequestOperation<T> extends UnicastOperation<T> {
    private final RequestFuture<T> future = new RequestFuture<>();

    private final RequestRetryCondition<T> until;

    public RequestOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        RequestRetryCondition<T> until
    ) {
        super(message, affinityKey, gateway, opts, false);

        this.until = until;
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
    public RetryDecision shouldRetry(Throwable error, ResponsePart<T> response) {
        if (until == null) {
            return RetryDecision.USE_DEFAULTS;
        }

        return until.accept(error, response);
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
