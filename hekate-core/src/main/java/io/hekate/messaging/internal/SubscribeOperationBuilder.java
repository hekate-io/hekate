package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.unicast.RequestRetryCondition;
import io.hekate.messaging.unicast.Subscribe;
import io.hekate.messaging.unicast.SubscribeCallback;
import io.hekate.messaging.unicast.SubscribeFuture;
import java.util.ArrayList;
import java.util.List;

class SubscribeOperationBuilder<T> extends MessageOperationBuilder<T> implements Subscribe<T> {
    private RequestRetryCondition<T> condition;

    private Object affinity;

    public SubscribeOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Subscribe<T> until(RequestRetryCondition<T> condition) {
        this.condition = condition;

        return this;
    }

    @Override
    public Subscribe<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public SubscribeFuture<T> submit(SubscribeCallback<T> callback) {
        SubscribeOperation<T> op = new SubscribeOperation<>(message(), affinity, gateway(), opts(), callback, condition);

        gateway().submit(op);

        return op.future();
    }

    @Override
    public List<T> responses() throws InterruptedException, MessagingFutureException {
        List<T> results = new ArrayList<>();

        SubscribeFuture<T> future = submit((err, rsp) -> {
            if (err == null) {
                results.add(rsp.payload());
            }
        });

        future.get();

        return results;
    }
}
