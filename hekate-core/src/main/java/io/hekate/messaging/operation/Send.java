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

package io.hekate.messaging.operation;

import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.concurrent.TimeUnit;

/**
 * <b>Send operation.</b>
 *
 * <p>
 * This interface represents a one-way message sending operation of a {@link MessagingChannel}.
 * </p>
 *
 * <h2>Acknowledgements</h2>
 * <p>
 * Messages can be sent with or without acknowledgements:
 * </p>
 * <ul>
 * <li>If acknowledgements are enabled then this operation will be completed only when an acknowledgement is received from the receiver</li>
 * <li>If acknowledgements are disabled then this operation will be completed immediately once it is flushed to the network buffer</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newSend(Object)} method call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withAckMode(AckMode) Acknowledgement Mode}</li>
 * <li>{@link #withTimeout(long, TimeUnit) Operation Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(SendRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit()} method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#send_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#send(Object)}</li>
 * <li>{@link MessagingChannel#send(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#send(Object, Object)}</li>
 * <li>{@link MessagingChannel#send(Object, Object, AckMode)}</li>
 * <li>{@link MessagingChannel#sendAsync(Object)}</li>
 * <li>{@link MessagingChannel#sendAsync(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#sendAsync(Object, Object)}</li>
 * <li>{@link MessagingChannel#sendAsync(Object, Object, AckMode)}</li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Send<T> {
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
    Send<T> withAffinity(Object affinity);

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
    Send<T> withTimeout(long timeout, TimeUnit unit);

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
    Send<T> withAckMode(AckMode ackMode);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     */
    Send<T> withRetry(SendRetryConfigurer retry);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    SendFuture submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(SendCallback callback) {
        ArgAssert.notNull(callback, "Callback");

        submit().whenComplete((ignore, err) ->
            callback.onComplete(err)
        );
    }

    /**
     * Synchronously executes this operation.
     *
     * @throws HekateException If execution fails.
     */
    default void sync() throws HekateException {
        submit().sync();
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#REQUIRED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Send<T> withAck() {
        return withAckMode(AckMode.REQUIRED);
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#NOT_NEEDED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Send<T> withNoAck() {
        return withAckMode(AckMode.NOT_NEEDED);
    }
}
