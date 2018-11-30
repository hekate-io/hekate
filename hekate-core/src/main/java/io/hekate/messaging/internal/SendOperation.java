package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.operation.SendAckMode;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;

class SendOperation<T> extends UnicastOperation<T> {
    private final SendFuture future = new SendFuture();

    private final SendAckMode ackMode;

    public SendOperation(
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
        SendAckMode ackMode
    ) {
        super(
            message,
            affinityKey,
            timeout,
            maxAttempts,
            retryErr,
            retryCondition,
            retryBackoff,
            retryCallback,
            retryRoute,
            gateway,
            opts,
            false
        );

        this.ackMode = ackMode;
    }

    @Override
    public OutboundType type() {
        return ackMode == SendAckMode.REQUIRED ? OutboundType.SEND_WITH_ACK : OutboundType.SEND_NO_ACK;
    }

    @Override
    public SendFuture future() {
        return future;
    }

    @Override
    public boolean shouldRetry(ResponsePart<T> response) {
        return false;
    }

    @Override
    protected void doReceiveFinal(ResponsePart<T> response) {
        future.complete(null);
    }

    @Override
    protected void doFail(Throwable error) {
        future.completeExceptionally(error);
    }
}
