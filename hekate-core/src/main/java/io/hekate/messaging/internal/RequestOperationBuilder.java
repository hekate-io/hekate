package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.RequestCondition;
import io.hekate.messaging.unicast.RequestFuture;

class RequestOperationBuilder<T> extends MessageOperationBuilder<T> implements Request<T> {
    private RequestCondition<T> condition;

    private Object affinity;

    public RequestOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Request<T> until(RequestCondition<T> condition) {
        this.condition = condition;

        return this;
    }

    @Override
    public Request<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public RequestFuture<T> submit() {
        RequestOperation<T> op = new RequestOperation<>(message(), affinity, gateway(), opts(), condition);

        gateway().submit(op);

        return op.future();
    }
}
