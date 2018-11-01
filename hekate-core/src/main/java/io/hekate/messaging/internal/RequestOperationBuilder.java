package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.RequestRetryCondition;

class RequestOperationBuilder<T> extends MessageOperationBuilder<T> implements Request<T> {
    private RequestRetryCondition<T> until;

    private Object affinity;

    public RequestOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Request<T> until(RequestRetryCondition<T> until) {
        this.until = until;

        return this;
    }

    @Override
    public Request<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public RequestFuture<T> submit() {
        RequestOperation<T> op = new RequestOperation<>(message(), affinity, gateway(), opts(), until);

        gateway().submit(op);

        return op.future();
    }
}
