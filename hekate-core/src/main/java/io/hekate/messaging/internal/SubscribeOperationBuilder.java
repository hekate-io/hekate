package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryResponsePolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.messaging.unicast.RequestRetryConfigurer;
import io.hekate.messaging.unicast.RequestRetryPolicy;
import io.hekate.messaging.unicast.Subscribe;
import io.hekate.messaging.unicast.SubscribeCallback;
import io.hekate.messaging.unicast.SubscribeFuture;
import java.util.ArrayList;
import java.util.List;

class SubscribeOperationBuilder<T> extends MessageOperationBuilder<T> implements Subscribe<T>, RequestRetryPolicy<T> {
    private Object affinity;

    private RetryErrorPolicy retryErr;

    private RetryResponsePolicy<T> retryResp;

    private RetryCondition retryCondition;

    private RetryCallback retryCallback;

    private RetryRoutingPolicy retryRoute = RetryRoutingPolicy.defaultPolicy();

    private int maxAttempts;

    public SubscribeOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Subscribe<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Subscribe<T> withRetry(RequestRetryConfigurer<T> retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPolicy.alwaysRetry();

        retry.configure(this);

        return this;
    }

    @Override
    public SubscribeFuture<T> submit(SubscribeCallback<T> callback) {
        SubscribeOperation<T> op = new SubscribeOperation<>(
            message(),
            affinity,
            maxAttempts,
            retryErr,
            retryResp,
            retryCondition,
            retryCallback,
            retryRoute,
            gateway(),
            opts(),
            callback
        );

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

    @Override
    public RequestRetryPolicy<T> whileResponse(RetryResponsePolicy<T> policy) {
        this.retryResp = policy;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> whileTrue(RetryCondition condition) {
        this.retryCondition = condition;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> whileError(RetryErrorPolicy policy) {
        this.retryErr = policy;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> onRetry(RetryCallback callback) {
        this.retryCallback = callback;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> route(RetryRoutingPolicy policy) {
        ArgAssert.notNull(policy, "Routing policy");

        this.retryRoute = policy;

        return this;
    }
}
