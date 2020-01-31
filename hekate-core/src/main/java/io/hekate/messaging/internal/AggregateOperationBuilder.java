package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.AggregateRepeatCondition;
import io.hekate.messaging.operation.AggregateRetryConfigurer;
import io.hekate.messaging.operation.AggregateRetryPolicy;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryResponsePredicate;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.hekate.messaging.internal.BroadcastOperationBuilder.nodesForBroadcast;

class AggregateOperationBuilder<T> extends MessageOperationBuilder<T> implements Aggregate<T>, AggregateRetryPolicy<T> {
    private Object affinity;

    private long timeout;

    private RetryErrorPredicate retryErr;

    private RetryResponsePredicate<T> retryResp;

    private RetryCondition retryCondition;

    private RetryBackoffPolicy retryBackoff;

    private RetryCallback retryCallback;

    private int maxAttempts;

    private AggregateRepeatCondition<T> repeat;

    public AggregateOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);

        this.timeout = gateway.messagingTimeout();
    }

    @Override
    public Aggregate<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Aggregate<T> withTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);

        return this;
    }

    @Override
    public Aggregate<T> withRetry(AggregateRetryConfigurer<T> retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPredicate.acceptAll();

        retry.configure(this);

        return this;
    }

    @Override
    public Aggregate<T> withRepeat(AggregateRepeatCondition<T> condition) {
        ArgAssert.notNull(condition, "Repeat condition");

        repeat = condition;

        return this;
    }

    @Override
    public AggregateFuture<T> submit() {
        AggregateFuture<T> future = new AggregateFuture<>();

        // Use a static method to make sure that we immutably capture all current settings of this operation.
        doSubmit(
            message(),
            affinity,
            timeout,
            maxAttempts,
            retryErr,
            retryResp,
            retryCondition,
            retryBackoff,
            retryCallback,
            repeat,
            gateway(),
            opts(),
            future
        );

        return future;
    }

    @Override
    public AggregateRetryPolicy<T> withBackoff(RetryBackoffPolicy backoff) {
        ArgAssert.notNull(backoff, "Backoff policy");

        this.retryBackoff = backoff;

        return this;
    }

    @Override
    public AggregateRetryPolicy<T> whileTrue(RetryCondition condition) {
        this.retryCondition = condition;

        return this;
    }

    @Override
    public AggregateRetryPolicy<T> whileError(RetryErrorPredicate policy) {
        this.retryErr = policy;

        return this;
    }

    @Override
    public AggregateRetryPolicy<T> whileResponse(RetryResponsePredicate<T> policy) {
        this.retryResp = policy;

        return this;
    }

    @Override
    public AggregateRetryPolicy<T> onRetry(RetryCallback callback) {
        this.retryCallback = callback;

        return this;
    }

    @Override
    public AggregateRetryPolicy<T> maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;

        return this;
    }

    // Static to make sure that we capture all the parameter at once and do not depend on the operation builder's state changes.
    private static <T> void doSubmit(
        T msg,
        Object affinity,
        long timeout,
        int maxAttempts,
        RetryErrorPredicate retryErr,
        RetryResponsePredicate<T> retryRsp,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        AggregateRepeatCondition<T> repeat,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        AggregateFuture<T> future
    ) {
        List<ClusterNode> nodes = nodesForBroadcast(affinity, opts);

        if (nodes.isEmpty()) {
            future.complete(new EmptyAggregateResult<>(msg));
        } else {
            AggregateContext<T> ctx = new AggregateContext<>(msg, nodes, future);

            nodes.forEach(node -> {
                AggregateOperation<T> op = new AggregateOperation<>(
                    msg,
                    affinity,
                    timeout,
                    maxAttempts,
                    retryErr,
                    retryRsp,
                    retryCondition,
                    retryBackoff,
                    retryCallback,
                    gateway,
                    opts,
                    node
                );

                gateway.submit(op);

                op.future().whenComplete((result, err) -> {
                    boolean complete;

                    if (err == null) {
                        complete = ctx.onReplySuccess(node, result);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before
                        // we've started the operation (note that no messages had been submitted to that node).
                        complete = ctx.forgetNode(node);
                    } else {
                        complete = ctx.onReplyFailure(node, err);
                    }

                    if (complete) {
                        if (repeat == null || gateway.isClosed() || ctx.isTimedOut() || !repeat.shouldRepeat(ctx)) {
                            // Aggregation is complete.
                            ctx.complete();
                        } else {
                            // Repeat aggregation.
                            doSubmit(
                                msg,
                                affinity,
                                timeout,
                                maxAttempts,
                                retryErr,
                                retryRsp,
                                retryCondition,
                                retryBackoff,
                                retryCallback,
                                repeat,
                                gateway,
                                opts,
                                future
                            );
                        }
                    }
                });
            });
        }
    }
}
