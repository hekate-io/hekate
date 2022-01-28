/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * <b>Aggregate operation.</b>
 *
 * <p>
 * This interface represents a request/response-style broadcast operation of a {@link MessagingChannel}.
 * This operation submits the same request message to multiple remote nodes and aggregates their responses.
 * </p>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newAggregate(Object)} method call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withTimeout(long, TimeUnit) Request Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(AggregateRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit()} method</li>
 * <li>Process results (synchronously or asynchronously)</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#aggregate_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#aggregate(Object)}</li>
 * <li>{@link MessagingChannel#aggregate(Object, Object)}</li>
 * <li>{@link MessagingChannel#aggregateAsync(Object)}</li>
 * <li>{@link MessagingChannel#aggregateAsync(Object, Object)}</li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Aggregate<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread (if the cluster topology doesn't change).
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Aggregate<T> withAffinity(Object affinity);

    /**
     * Overrides the channel's default timeout value for this operation.
     *
     * <p>
     * If this operation can not complete at the specified timeout then this operation will end up in a {@link MessageTimeoutException}.
     * </p>
     *
     * <p>
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * @param timeout Timeout.
     * @param unit Unit.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setMessagingTimeout(long)
     */
    Aggregate<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy for each individual message of the aggregation operation.
     *
     * <p>
     * This policy gets applied to each individual (per-node) message. For example, if aggregation should be performed over 3 nodes
     * and an error happens while sending an aggregation message to the second node then this retry policy will be applied only to that
     * particular failed message.
     * </p>
     *
     * <p>
     * If retry logic should be implemented at the aggregation level (i.e. decide on whether to retry or not by looking at the whole
     * {@link AggregateResult}) please see the {@link #withRepeat(AggregateRepeatCondition)} method.
     * </p>
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     * @see #withRepeat(AggregateRepeatCondition)
     */
    Aggregate<T> withRetry(AggregateRetryConfigurer<T> retry);

    /**
     * Condition to repeat for the whole {@link Aggregate} operation.
     *
     * <p>
     * If the specified condition evaluates to {@code true} then the whole {@link Aggregate} operation will be repeated from scratch,
     * except for the following cases:
     * </p>
     *
     * <ul>
     *    <li>{@link Aggregate} operation timed out (see {@link #withTimeout(long, TimeUnit)})</li>
     *    <li>{@link MessagingChannel}'s cluster topology is empty (i.e. no nodes to perform aggregation)</li>
     *    <li>{@link Hekate} node is stopped</li>
     * </ul>
     *
     * <p>
     *  If one of the above is true then {@link Aggregate} will complete with whatever {@link AggregateResult} it has.
     * </p>
     *
     * @param condition Condition.
     *
     * @return This instance.
     *
     * @see #withRetry(AggregateRetryConfigurer)
     */
    Aggregate<T> withRepeat(AggregateRepeatCondition<T> condition);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    AggregateFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(AggregateCallback<T> callback) {
        submit().whenComplete((rslt, err) ->
            callback.onComplete(err, rslt)
        );
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result (see {@link AggregateResult#results()}).
     *
     * @throws HekateException If operations fails.
     */
    default Collection<T> results() throws HekateException {
        return submit().sync().results();
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws HekateException If operations fails.
     */
    default AggregateResult<T> get() throws HekateException {
        return submit().sync();
    }
}
