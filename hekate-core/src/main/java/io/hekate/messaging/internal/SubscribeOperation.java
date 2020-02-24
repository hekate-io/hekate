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

import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.operation.SubscribeCallback;
import io.hekate.messaging.operation.SubscribeFuture;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryResponsePredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;

class SubscribeOperation<T> extends UnicastOperation<T> {
    private final SubscribeFuture<T> future = new SubscribeFuture<>();

    private final SubscribeCallback<T> callback;

    private final RetryResponsePredicate<T> retryRsp;

    private volatile boolean active;

    public SubscribeOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPredicate retryErr,
        RetryResponsePredicate<T> retryRsp,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        SubscribeCallback<T> callback
    ) {
        super(
            message,
            affinityKey,
            timeout,
            maxAttempts,
            retryErr,
            retryCondition,
            retryBackoff,
            retryCallback,
            retryRoute,
            gateway,
            opts,
            true
        );

        this.callback = callback;
        this.retryRsp = retryRsp;
    }

    @Override
    public OutboundType type() {
        return OutboundType.SUBSCRIBE;
    }

    @Override
    public SubscribeFuture<T> future() {
        return future;
    }

    @Override
    public boolean shouldRetry(ResponsePart<T> response) {
        return retryRsp != null
            && !isPartial(response)
            && retryRsp.shouldRetry(response);
    }

    @Override
    public boolean shouldExpireOnTimeout() {
        if (active) {
            // Reset the flag so that we could detect inactivity upon the next invocation of this method.
            active = false;

            return false;
        } else {
            // No activity between this invocation and the previous one.
            return true;
        }
    }

    @Override
    protected void doReceivePartial(ResponsePart<T> response) {
        if (!active) {
            active = true;
        }

        callback.onComplete(null, response);
    }

    @Override
    protected void doReceiveFinal(ResponsePart<T> response) {
        try {
            callback.onComplete(null, response);
        } finally {
            future.complete(response);
        }
    }

    @Override
    protected void doFail(Throwable error) {
        try {
            callback.onComplete(error, null);
        } finally {
            future.completeExceptionally(error);
        }
    }

    private static boolean isPartial(ResponsePart<?> response) {
        return response != null && !response.isLastPart();
    }
}
