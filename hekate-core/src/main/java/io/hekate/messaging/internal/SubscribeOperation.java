package io.hekate.messaging.internal;

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestCondition;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.SubscribeFuture;

class SubscribeOperation<T> extends UnicastOperation<T> {
    private final SubscribeFuture<T> future = new SubscribeFuture<>();

    private final RequestCallback<T> callback;

    private final RequestCondition<T> condition;

    private volatile boolean active;

    public SubscribeOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        RequestCallback<T> callback,
        RequestCondition<T> condition
    ) {
        super(message, affinityKey, gateway, opts, true);

        this.callback = callback;
        this.condition = condition;
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
    public ReplyDecision accept(Throwable error, Response<T> response) {
        if (condition == null || isPartial(response)) {
            return ReplyDecision.DEFAULT;
        }

        return condition.accept(error, response);
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
    protected void doReceivePartial(Response<T> response) {
        if (!active) {
            active = true;
        }

        callback.onComplete(null, response);
    }

    @Override
    protected void doReceiveFinal(Response<T> response) {
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

    private static boolean isPartial(Response<?> response) {
        return response != null && response.isPartial();
    }
}
