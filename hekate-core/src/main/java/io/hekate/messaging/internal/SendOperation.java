package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.messaging.unicast.ResponsePart;
import io.hekate.messaging.unicast.SendAckMode;
import io.hekate.messaging.unicast.SendFuture;

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
