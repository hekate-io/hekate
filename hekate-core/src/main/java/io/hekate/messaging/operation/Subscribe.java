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

import io.hekate.core.HekateException;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <b>Subscribe operation.</b>
 *
 * <p>
 * This interface represents a subscribe operation of a {@link MessagingChannel}. This operation submits a single request (subscription) and
 * expects multiple response chunks (updates) to be returned by the receiver.
 * </p>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newSubscribe(Object)} call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withTimeout(long, TimeUnit) Operation Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(RequestRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit(SubscribeCallback)}  method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#subscribe_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#subscribe(Object)}</li>
 * <li>{@link MessagingChannel#subscribe(Object, Object)} </li>
 * <li>{@link MessagingChannel#subscribeAsync(Object, SubscribeCallback)}</li>
 * <li>{@link MessagingChannel#subscribeAsync(Object, Object, SubscribeCallback)} </li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Subscribe<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread (if the cluster topology doesn't change).
     * </p>
     *
     * <p>
     * {@link LoadBalancer} can also make use of the affinity key in order to perform consistent routing of messages among the cluster
     * node. For example, the default load balancer makes sure that all messages, having the same key, are always routed to the same node
     * (unless the cluster topology doesn't change).
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Subscribe<T> withAffinity(Object affinity);

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
    Subscribe<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     */
    Subscribe<T> withRetry(RequestRetryConfigurer<T> retry);

    /**
     * Asynchronously executes this operation.
     *
     * @param callback Callback.
     *
     * @return Future result of this operation.
     */
    SubscribeFuture<T> submit(SubscribeCallback<T> callback);

    /**
     * Synchronously collects all response chunks.
     *
     * <p>
     * This method submits the subscription operation and blocks util all {@link Message#partialReply(Object) responses}) are received.
     * All responses will be collected into an in-memory list, hence this method should be used with caution (mostly for testing purposes).
     * </p>
     *
     * @return List of all {@link Message#partialReply(Object) partial responses} and the {@link Message#reply(Object) final response}.
     *
     * @throws HekateException If operation failed.
     */
    List<T> responses() throws HekateException;
}
