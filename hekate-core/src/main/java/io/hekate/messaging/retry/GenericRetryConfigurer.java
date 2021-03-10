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

package io.hekate.messaging.retry;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.AggregateRetryConfigurer;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.BroadcastRetryConfigurer;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.RequestRetryConfigurer;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.SendRetryConfigurer;

/**
 * Generic retry policy for a messaging channel.
 *
 * <p>
 * Instances of this interface are responsible for providing a default retry policy for messaging operations of a {@link MessagingChannel}
 * and can be registered via the {@link MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)}.
 * </p>
 *
 * <p>
 * This policy can be overridden for each individual operation via the following methods:
 * </p>
 * <ul>
 * <li>{@link Request#withRetry(RequestRetryConfigurer)}</li>
 * <li>{@link Send#withRetry(SendRetryConfigurer)}</li>
 * <li>{@link Broadcast#withRetry(BroadcastRetryConfigurer)}</li>
 * <li>{@link Aggregate#withRetry(AggregateRetryConfigurer)}</li>
 * </ul>
 *
 * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
 */
@FunctionalInterface
public interface GenericRetryConfigurer {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(RetryPolicy<?> retry);

    /**
     * Do not retry.
     *
     * @return Policy that doesn't retry failed operations.
     */
    static GenericRetryConfigurer noRetries() {
        return retry -> {
            // No-op.
        };
    }
}
