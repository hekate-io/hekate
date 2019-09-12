package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.operation.SendRetryConfigurer;
import io.hekate.messaging.operation.SendRetryPolicy;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import java.util.concurrent.TimeUnit;

class SendOperationBuilder<T> extends MessageOperationBuilder<T> implements Send<T>, SendRetryPolicy {
    private Object affinity;

    private AckMode ackMode;

    private RetryErrorPredicate retryErr;

    private RetryCondition retryCondition;

    private RetryBackoffPolicy retryBackoff;

    private RetryCallback retryCallback;

    private RetryRoutingPolicy retryRoute = RetryRoutingPolicy.defaultPolicy();

    private int maxAttempts;

    private long timeout;

    public SendOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);

        this.timeout = gateway.messagingTimeout();
    }

    @Override
    public Send<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Send<T> withTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);

        return this;
    }

    @Override
    public Send<T> withAckMode(AckMode ackMode) {
        ArgAssert.notNull(ackMode, "Acknowledgement mode");

        this.ackMode = ackMode;

        return this;
    }

    @Override
    public Send<T> withRetry(SendRetryConfigurer retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPredicate.acceptAll();

        retry.configure(this);

        return this;
    }

    @Override
    public SendFuture submit() {
        SendOperation<T> op = new SendOperation<>(
            message(),
            affinity,
            timeout,
            maxAttempts,
            retryErr,
            retryCondition,
            retryBackoff,
            retryCallback,
            retryRoute,
            gateway(),
            opts(),
            ackMode
        );

        gateway().submit(op);

        return op.future();
    }

    @Override
    public SendRetryPolicy route(RetryRoutingPolicy policy) {
        ArgAssert.notNull(policy, "Routing policy");

        this.retryRoute = policy;

        return this;
    }

    @Override
    public SendRetryPolicy withBackoff(RetryBackoffPolicy backoff) {
        ArgAssert.notNull(backoff, "Backoff policy");

        this.retryBackoff = backoff;

        return this;
    }

    @Override
    public SendRetryPolicy whileTrue(RetryCondition condition) {
        this.retryCondition = condition;

        return this;
    }

    @Override
    public SendRetryPolicy whileError(RetryErrorPredicate policy) {
        this.retryErr = policy;

        return this;
    }

    @Override
    public SendRetryPolicy onRetry(RetryCallback callback) {
        this.retryCallback = callback;

        return this;
    }

    @Override
    public SendRetryPolicy maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;

        return this;
    }
}
