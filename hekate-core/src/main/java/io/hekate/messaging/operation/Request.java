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

import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.concurrent.TimeUnit;

/**
 * <b>Request operation.</b>
 *
 * <p>
 * This interface represents a request/response operation of a {@link MessagingChannel}.
 * </p>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newRequest(Object)} method call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withTimeout(long, TimeUnit) Request Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(RequestRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit()} method</li>
 * <li>Process the response (synchronously or asynchronously)</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#request_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#request(Object)}</li>
 * <li>{@link MessagingChannel#request(Object, Object)}</li>
 * <li>{@link MessagingChannel#requestAsync(Object)}</li>
 * <li>{@link MessagingChannel#requestAsync(Object, Object)}</li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Request<T> {
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
    Request<T> withAffinity(Object affinity);

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
    Request<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     */
    Request<T> withRetry(RequestRetryConfigurer<T> retry);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    RequestFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(RequestCallback<T> callback) {
        submit().whenComplete((rsp, err) ->
            callback.onComplete(err, rsp)
        );
    }

    /**
     * Synchronously executes this operation and returns the response's payload.
     *
     * @return Response's payload (see {@link Response#payload()}).
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default T response() throws MessagingFutureException, InterruptedException {
        return submit().get().payload();
    }

    /**
     * Synchronously executes this operation and returns the response.
     *
     * @param <R> Response type.
     * @param responseType Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default <R extends T> R response(Class<R> responseType) throws MessagingFutureException, InterruptedException {
        return submit().get().payload(responseType);
    }

    /**
     * Synchronously executes this operation and returns the response.
     *
     * @return Response.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default Response<T> get() throws MessagingFutureException, InterruptedException {
        return submit().get();
    }
}
