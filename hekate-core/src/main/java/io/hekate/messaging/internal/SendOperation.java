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
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;

class SendOperation<T> extends UnicastOperation<T> {
    private final SendFuture future = new SendFuture();

    private final AckMode ackMode;

    public SendOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPredicate retryErr,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        AckMode ackMode
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
            false
        );

        this.ackMode = ackMode;
    }

    @Override
    public OutboundType type() {
        return ackMode == AckMode.REQUIRED ? OutboundType.SEND_WITH_ACK : OutboundType.SEND_NO_ACK;
    }

    @Override
    public SendFuture future() {
        return future;
    }

    @Override
    public boolean shouldRetry(ResponsePart<T> response) {
        return false;
    }

    @Override
    protected void doReceiveFinal(ResponsePart<T> response) {
        future.complete(null);
    }

    @Override
    protected void doFail(Throwable error) {
        future.completeExceptionally(error);
    }
}
