/*
 * Copyright 2020 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.BroadcastRepeatCondition;
import io.hekate.messaging.operation.BroadcastRetryConfigurer;
import io.hekate.messaging.operation.BroadcastRetryPolicy;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import java.util.List;
import java.util.concurrent.TimeUnit;

class BroadcastOperationBuilder<T> extends MessageOperationBuilder<T> implements Broadcast<T>, BroadcastRetryPolicy {
    private Object affinity;

    private AckMode ackMode = AckMode.NOT_NEEDED;

    private long timeout;

    private RetryErrorPredicate retryErr;

    private RetryBackoffPolicy retryBackoff;

    private RetryCondition retryCondition;

    private RetryCallback retryCallback;

    private int maxAttempts;

    private BroadcastRepeatCondition<T> repeat;

    public BroadcastOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);

        this.timeout = gateway.messagingTimeout();
    }

    @Override
    public Broadcast<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Broadcast<T> withTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);

        return this;
    }

    @Override
    public Broadcast<T> withAckMode(AckMode ackMode) {
        ArgAssert.notNull(ackMode, "Acknowledgement mode");

        this.ackMode = ackMode;

        return this;
    }

    @Override
    public Broadcast<T> withRetry(BroadcastRetryConfigurer retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPredicate.acceptAll();

        retry.configure(this);

        return this;
    }

    @Override
    public Broadcast<T> withRepeat(BroadcastRepeatCondition<T> condition) {
        ArgAssert.notNull(condition, "Repeat condition");

        repeat = condition;

        return this;
    }

    @Override
    public BroadcastFuture<T> submit() {
        BroadcastFuture<T> future = new BroadcastFuture<>();

        // Use a static method to make sure that we immutably capture all current settings of this operation.
        doSubmit(
            message(),
            affinity,
            timeout,
            maxAttempts,
            ackMode,
            retryErr,
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
    public BroadcastRetryPolicy withBackoff(RetryBackoffPolicy backoff) {
        ArgAssert.notNull(backoff, "Backoff policy");

        this.retryBackoff = backoff;

        return this;
    }

    @Override
    public BroadcastRetryPolicy whileTrue(RetryCondition condition) {
        this.retryCondition = condition;

        return this;
    }

    @Override
    public BroadcastRetryPolicy whileError(RetryErrorPredicate policy) {
        this.retryErr = policy;

        return this;
    }

    @Override
    public BroadcastRetryPolicy onRetry(RetryCallback callback) {
        this.retryCallback = callback;

        return this;
    }

    @Override
    public BroadcastRetryPolicy maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;

        return this;
    }

    static List<ClusterNode> nodesForBroadcast(Object affinityKey, MessageOperationOpts<?> opts) {
        List<ClusterNode> nodes;

        if (affinityKey == null) {
            // Use the whole topology if affinity key is not specified.
            nodes = opts.cluster().topology().nodes();
        } else {
            // Use only those nodes that are mapped to the affinity key.
            nodes = opts.partitions().map(affinityKey).nodes();
        }

        return nodes;
    }

    // Static to make sure that we capture all the parameter at once and do not depend on the operation builder's state changes.
    private static <T> void doSubmit(
        T msg,
        Object affinity,
        long timeout,
        int maxAttempts,
        AckMode ackMode,
        RetryErrorPredicate retry,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        BroadcastRepeatCondition<T> repeat,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        BroadcastFuture<T> future
    ) {
        List<ClusterNode> nodes = nodesForBroadcast(affinity, opts);

        if (nodes.isEmpty()) {
            future.complete(new EmptyBroadcastResult<>(msg));
        } else {
            BroadcastContext<T> ctx = new BroadcastContext<>(msg, nodes, future);

            nodes.forEach(node -> {
                BroadcastOperation<T> op = new BroadcastOperation<>(
                    msg,
                    affinity,
                    timeout,
                    maxAttempts,
                    retry,
                    retryCondition,
                    retryBackoff,
                    retryCallback,
                    gateway,
                    opts,
                    ackMode,
                    node
                );

                gateway.submit(op);

                op.future().whenComplete((ignore, err) -> {
                    boolean complete;

                    if (err == null) {
                        complete = ctx.onSendSuccess();
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before
                        // we've started the operation (note that no messages had been submitted to that node).
                        complete = ctx.forgetNode(node);
                    } else {
                        complete = ctx.onSendFailure(node, err);
                    }

                    if (complete) {
                        if (repeat == null || gateway.isClosed() || ctx.isTimedOut() || !repeat.shouldRepeat(ctx)) {
                            // Broadcast is complete.
                            ctx.complete();
                        } else {
                            // Repeat broadcast.
                            doSubmit(
                                msg,
                                affinity,
                                timeout,
                                maxAttempts,
                                ackMode,
                                retry,
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
