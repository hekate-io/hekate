/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.operation.RequestRetryConfigurer;
import io.hekate.messaging.operation.RequestRetryPolicy;
import io.hekate.messaging.operation.Subscribe;
import io.hekate.messaging.operation.SubscribeCallback;
import io.hekate.messaging.operation.SubscribeFuture;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryResponsePredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class SubscribeOperationBuilder<T> extends MessageOperationBuilder<T> implements Subscribe<T>, RequestRetryPolicy<T> {
    private Object affinity;

    private RetryErrorPredicate retryErr;

    private RetryResponsePredicate<T> retryResp;

    private RetryCondition retryCondition;

    private RetryBackoffPolicy retryBackoff;

    private RetryCallback retryCallback;

    private RetryRoutingPolicy retryRoute = RetryRoutingPolicy.defaultPolicy();

    private int maxAttempts;

    private long timeout;

    public SubscribeOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);

        this.timeout = gateway.messagingTimeout();
    }

    @Override
    public Subscribe<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Subscribe<T> withTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);

        return this;
    }

    @Override
    public Subscribe<T> withRetry(RequestRetryConfigurer<T> retry) {
        ArgAssert.notNull(retry, "Retry policy");

        // Make sure that by default we retry all errors.
        retryErr = RetryErrorPredicate.acceptAll();

        retry.configure(this);

        return this;
    }

    @Override
    public SubscribeFuture<T> submit(SubscribeCallback<T> callback) {
        SubscribeOperation<T> op = new SubscribeOperation<>(
            message(),
            affinity,
            timeout,
            maxAttempts,
            retryErr,
            retryResp,
            retryCondition,
            retryBackoff,
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
    public List<T> responses() {
        List<T> results = new ArrayList<>();

        SubscribeFuture<T> future = submit((err, rsp) -> {
            if (err == null) {
                results.add(rsp.payload());
            }
        });

        future.sync();

        return results;
    }

    @Override
    public RequestRetryPolicy<T> withBackoff(RetryBackoffPolicy backoff) {
        ArgAssert.notNull(backoff, "Backoff policy");

        this.retryBackoff = backoff;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> whileResponse(RetryResponsePredicate<T> policy) {
        this.retryResp = policy;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> whileTrue(RetryCondition condition) {
        this.retryCondition = condition;

        return this;
    }

    @Override
    public RequestRetryPolicy<T> whileError(RetryErrorPredicate policy) {
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
