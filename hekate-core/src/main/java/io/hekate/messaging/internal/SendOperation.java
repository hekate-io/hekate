package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.RetryDecision;
import io.hekate.messaging.unicast.SendAckMode;
import io.hekate.messaging.unicast.SendFuture;

class SendOperation<T> extends UnicastOperation<T> {
    private final SendFuture future = new SendFuture();

    private final SendAckMode ackMode;

    public SendOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        SendAckMode ackMode
    ) {
        super(message, affinityKey, gateway, opts, false);

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
    public RetryDecision shouldRetry(Throwable error, Response<T> response) {
        return RetryDecision.USE_DEFAULTS;
    }

    @Override
    protected void doReceiveFinal(Response<T> response) {
        future.complete(null);
    }

    @Override
    protected void doFail(Throwable error) {
        future.completeExceptionally(error);
    }
}
