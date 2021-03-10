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

import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.concurrent.TimeUnit;

/**
 * <b>Broadcast operation.</b>
 *
 * <p>
 * This interface represents a one-way message broadcast operation of a {@link MessagingChannel}.
 * This operation submits the same message to multiple remote nodes.
 * </p>
 *
 * <h2>Acknowledgements</h2>
 * <p>
 * Messages can be broadcast with or without acknowledgements:
 * </p>
 * <ul>
 * <li>If acknowledgements are enabled then this operation will be completed only when acknowledgements are received from all receiver</li>
 * <li>If acknowledgements are disabled then this operation will be completed immediately once it is flushed to the network buffer</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newBroadcast(Object)} method call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withAckMode(AckMode) Acknowledgememt Mode}</li>
 * <li>{@link #withTimeout(long, TimeUnit) Operation Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(BroadcastRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit()} method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#broadcast_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#broadcast(Object)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, Object)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, Object)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, Object, AckMode)}</li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Broadcast<T> {
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
    Broadcast<T> withAffinity(Object affinity);

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
    Broadcast<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Acknowledgement mode.
     *
     * <p>
     * If this option is set to {@link AckMode#REQUIRED} then the receiver of this operation will send back an acknowledgement to
     * indicate that this operation was successfully {@link MessageReceiver#receive(Message) received}. In such case the operation's
     * callback/future will be notified only when such acknowledgement is received from all nodes (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@link AckMode#NOT_NEEDED} then operation will be assumed to be successful once the message gets
     * flushed to the network buffer without any additional acknowledgements from receivers.
     * </p>
     *
     * <p>
     * Default value of this option is {@link AckMode#NOT_NEEDED}.
     * </p>
     *
     * @param ackMode Acknowledgement mode.
     *
     * @return This instance.
     */
    Broadcast<T> withAckMode(AckMode ackMode);

    /**
     * Retry policy.
     *
     * <p>
     * This policy gets applied to each individual (per-node) message. For example, if message should be broadcasted to 3 nodes
     * and an error happens while sending the message to the second node then this retry policy will be applied only to that
     * particular failed message.
     * </p>
     *
     * <p>
     * If retry logic should be implemented at the broadcast level (i.e. decide on whether to retry or not by looking at the whole
     * {@link BroadcastResult}) please see the {@link #withRepeat(BroadcastRepeatCondition)} method.
     * </p>
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     */
    Broadcast<T> withRetry(BroadcastRetryConfigurer retry);

    /**
     * Condition to repeat for the whole {@link Broadcast} operation.
     *
     * <p>
     * If the specified condition evaluates to {@code true} then the whole {@link Broadcast} operation will be repeated from scratch,
     * except for the following cases:
     * </p>
     *
     * <ul>
     *    <li>{@link Broadcast} operation timed out (see {@link #withTimeout(long, TimeUnit)})</li>
     *    <li>{@link MessagingChannel}'s cluster topology is empty (i.e. no nodes to broadcast to)</li>
     *    <li>{@link Hekate} node is stopped</li>
     * </ul>
     *
     * <p>
     *  If one of the above is true then {@link Broadcast} will complete with whatever {@link BroadcastResult} it has.
     * </p>
     *
     * @param condition Condition.
     *
     * @return This instance.
     *
     * @see #withRetry(BroadcastRetryConfigurer)
     */
    Broadcast<T> withRepeat(BroadcastRepeatCondition<T> condition);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    BroadcastFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(BroadcastCallback<T> callback) {
        submit().whenComplete((result, error) ->
            callback.onComplete(error, result)
        );
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws HekateException If operations fails.
     */
    default BroadcastResult<T> sync() throws HekateException {
        return submit().sync();
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#REQUIRED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Broadcast<T> withAck() {
        return withAckMode(AckMode.REQUIRED);
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#NOT_NEEDED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Broadcast<T> withNoAck() {
        return withAckMode(AckMode.NOT_NEEDED);
    }
}
