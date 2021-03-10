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

package io.hekate.messaging.operation;

import io.hekate.messaging.retry.RetryPolicy;
import io.hekate.messaging.retry.RetryResponseSupport;

/**
 * Retry policy for an {@link Aggregate} operation.
 *
 * <p>
 * This policy gets applied to each individual (per-node) message. For example, if aggregation should be performed over 3 nodes
 * and an error happens while sending an aggregation message to the second node then this retry policy will be applied only to that
 * particular failed message.
 * </p>
 *
 * <p>
 * If retry logic should be implemented at the aggregation level (i.e. decide on whether to retry or not by looking at the whole
 * {@link AggregateResult}) please see the {@link Aggregate#withRepeat(AggregateRepeatCondition)} method.
 * </p>
 *
 * @param <T> Message type.
 *
 * @see Aggregate#withRetry(AggregateRetryConfigurer)
 */
public interface AggregateRetryPolicy<T> extends RetryPolicy<AggregateRetryPolicy<T>>, RetryResponseSupport<T, AggregateRetryPolicy<T>> {
    // No-op.
}
