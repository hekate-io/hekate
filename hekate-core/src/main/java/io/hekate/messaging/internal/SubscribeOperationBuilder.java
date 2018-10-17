package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestRetryCondition;
import io.hekate.messaging.unicast.Subscribe;
import io.hekate.messaging.unicast.SubscribeFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public SubscribeFuture<T> submit(RequestCallback<T> callback) {
        SubscribeOperation<T> op = new SubscribeOperation<>(message(), affinity, gateway(), opts(), callback, condition);

        gateway().submit(op);

        return op.future();
    }

    @Override
    public List<T> collectAll(long timeout, TimeUnit unit) throws InterruptedException, MessagingFutureException, TimeoutException {
        List<T> results = new ArrayList<>();

        SubscribeFuture<T> future = submit((err, rsp) -> {
            if (err == null) {
                results.add(rsp.get());
            }
        });

        future.get(timeout, unit);

        return results;
    }
}
