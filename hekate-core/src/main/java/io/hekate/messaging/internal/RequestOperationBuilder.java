package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryResponsePolicy;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.RequestRetryConfigurer;
import io.hekate.messaging.unicast.RequestRetryPolicy;

class RequestOperationBuilder<T> extends MessageOperationBuilder<T> implements Request<T>, RequestRetryPolicy<T> {
    private RetryResponsePolicy<T> retryRsp;

    private Object affinity;

    private RetryErrorPolicy retryErr;

    private RetryCondition retryCondition;

    private RetryCallback retryCallback;

    private RetryRoutingPolicy retryRoute = RetryRoutingPolicy.defaultPolicy();

    private int maxAttempts;

    public RequestOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Request<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Request<T> withRetry(RequestRetryConfigurer<T> retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPolicy.alwaysRetry();

        retry.configure(this);

        return this;
    }

    @Override
    public RequestFuture<T> submit() {
        RequestOperation<T> op = new RequestOperation<>(
            message(),
            affinity,
            maxAttempts,
            retryErr,
            retryRsp,
            retryCondition, 
            retryCallback,
            retryRoute,
            gateway(),
            opts()
        );

        gateway().submit(op);

        return op.future();
    }

    @Override
    public RequestRetryPolicy<T> whileResponse(RetryResponsePolicy<T> policy) {
        this.retryRsp = policy;

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
