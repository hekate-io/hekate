/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.messaging;

import io.hekate.cluster.ClusterFilterSupport;
import io.hekate.cluster.ClusterView;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Messaging channel.
 *
 * <p>
 * This interface represents a messaging channel and provides API for sending messages to remote nodes.
 * </p>
 *
 * <p>
 * Instances of this interface can be obtained via {@link MessagingService#channel(String)} method.
 * </p>
 *
 * <p>
 * For more details about messaging and channels please see the documentation of {@link MessagingService} interface.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by channels.
 *
 * @see MessagingService#channel(String)
 */
public interface MessagingChannel<T> extends ClusterFilterSupport<MessagingChannel<T>> {
    /**
     * Returns the universally unique identifier of this channel.
     *
     * @return Universally unique identifier of this channel.
     */
    MessagingChannelId id();

    /**
     * Returns the channel name.
     *
     * @return Channel name.
     *
     * @see MessagingChannelConfig#setName(String)
     */
    String name();

    /**
     * Returns the cluster view of this channel.
     *
     * @return Cluster view.
     */
    ClusterView cluster();

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections
     * (see {@link MessagingChannelConfig#setNioThreads(int)}).
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    int nioThreads();

    /**
     * Returns the worker thread pool size (see {@link MessagingChannelConfig#setWorkerThreads(int)}).
     *
     * @return Worker thread pool size.
     */
    int workerThreads();

    /**
     * Asynchronously sends a one way message and returns a future object that can be used to inspect the operation result.
     *
     * <p>
     * This method doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #request(Object)} method.
     * </p>
     *
     * @param message Message to be sent.
     *
     * @return Future object that can be used to inspect the operation result.
     */
    SendFuture send(T message);

    /**
     * Asynchronously sends a one way message and notifies the specified callback when the operation gets completed.
     *
     * <p>
     * This method doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #request(Object, ResponseCallback)} method.
     * </p>
     *
     * @param message Message to be sent.
     * @param callback Callback.
     */
    void send(T message, SendCallback callback);

    /**
     * Asynchronously sends a request message and returns a future object that will be completed after receiving the response.
     *
     * @param request Request message.
     * @param <R> Response type.
     *
     * @return Future object that can be used to obtain the response.
     */
    <R extends T> ResponseFuture<R> request(T request);

    /**
     * Asynchronously sends a request message and notifies the specified callback after receiving the response..
     *
     * @param request Request message.
     * @param callback Callback.
     */
    void request(T request, ResponseCallback<T> callback);

    /**
     * Opens a stream for receiving continuous responses.
     *
     * <p>
     * This method asynchronously sends a request message, opens a stream for receiving {@link Message#partialReply(Object) partial replies}
     * and returns a future object that will be completed after receiving <b>all</b> responses.
     * </p>
     *
     * <p>
     * <b>Notice:</b> this method performs in-memory buffering of all received messages until the last
     * (non-{@link Response#isPartial() partial} response is received.
     * </p>
     *
     * @param request Request.
     *
     * @return Future object that can be used to obtain all of the received responses.
     */
    SubscribeFuture<T> subscribe(T request);

    /**
     * Opens a stream for receiving continuous responses.
     *
     * <p>
     * This method asynchronously sends a request message and opens a stream for receiving {@link Message#partialReply(Object) partial
     * replies}. For each such reply the {@link ResponseCallback#onComplete(Throwable, Response)} method will be called until the last
     * (non-{@link Response#isPartial() partial}) response is received.
     * </p>
     *
     * @param request Request.
     * @param callback Callback.
     */
    void subscribe(T request, ResponseCallback<T> callback);

    /**
     * Asynchronously broadcasts the specified message and returns a future object that can be used to inspect the operation result.
     *
     * @param message Message to broadcast.
     *
     * @return Future object that can be used to inspect the broadcast operation result.
     */
    BroadcastFuture<T> broadcast(T message);

    /**
     * Asynchronously broadcasts the specified message and notifies the specified callback upon the operation completion.
     *
     * @param message Message to broadcast.
     * @param callback Callback that should be notified upon the broadcast operation completion.
     */
    void broadcast(T message, BroadcastCallback<T> callback);

    /**
     * Asynchronously sends the query message and aggregates responses from all the nodes that received this message. This method returns a
     * future object that can be used to inspect the aggregation results.
     *
     * @param message Query message that should be sent.
     * @param <R> Response type (each remote node should return a response of this type).
     *
     * @return Future object that can be used to inspect the aggregation results.
     *
     * @see #aggregate(Object, AggregateCallback)
     */
    <R extends T> AggregateFuture<R> aggregate(T message);

    /**
     * Asynchronously sends the query message, aggregates responses from all nodes that received this message and notifies the
     * specified callback on operation progress and aggregation results.
     *
     * @param message Query message that should be sent.
     * @param callback Callback that should be notified on operation progress and aggregation results.
     *
     * @see #aggregate(Object)
     */
    void aggregate(T message, AggregateCallback<T> callback);

    /**
     * Returns a copy of this channel that will apply the specified affinity key to all messaging operations.
     *
     * <p>
     * Specifying an affinity key ensures that all messages sent with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * @param affinityKey Affinity key (if {@code null} then affinity key will be cleared).
     *
     * @return Channel wrapper.
     */
    MessagingChannel<T> withAffinity(Object affinityKey);

    /**
     * Returns the affinity key that was set via {@link #withAffinity(Object)}.
     *
     * @return Affinity key or {@code null}, if no affinity is specified.
     */
    Object affinity();

    /**
     * Returns a copy of this channel that will use the specified failover policy and will inherit all other options from this instance.
     *
     * @param policy Failover policy.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setFailoverPolicy(FailoverPolicy)
     */
    MessagingChannel<T> withFailover(FailoverPolicy policy);

    /**
     * Returns a copy of this channel that will use the specified failover policy and will inherit all other options from this instance.
     *
     * @param policy Failover policy.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setFailoverPolicy(FailoverPolicy)
     */
    MessagingChannel<T> withFailover(FailoverPolicyBuilder policy);

    /**
     * Returns the failover policy that was set via {@link #withFailover(FailoverPolicy)}.
     *
     * @return Failover policy or {@code null}, if no policy is specified.
     */
    FailoverPolicy failover();

    /**
     * Returns a copy of this channel that will use the specified timeout value and will inherit all other options from this instance.
     *
     * <p>
     * If the message exchange operation can not be completed at the specified timeout then such operation will end up the error {@link
     * MessageTimeoutException}.
     * </p>
     *
     * <p>
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * @param timeout Timeout.
     * @param unit Unit.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setMessagingTimeout(long)
     */
    MessagingChannel<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Returns the timeout value in milliseconds.
     *
     * @return Timeout in milliseconds or 0, if timeout was not specified.
     *
     * @see #withTimeout(long, TimeUnit)
     */
    long timeout();

    /**
     * Returns a copy of this channel that will use the specified load balancer and will inherit all other options from this instance.
     *
     * @param balancer Load balancer.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setLoadBalancer(LoadBalancer)
     */
    MessagingChannel<T> withLoadBalancer(LoadBalancer<T> balancer);

    /**
     * Returns the asynchronous task executor of this channel.
     *
     * <p>
     * The returned executor can be a single-thread or a wrapper over the multi-threaded executor depending on the {@link
     * MessagingChannelConfig#setWorkerThreads(int)} value.
     * </p>
     *
     * @return Asynchronous task executor of this channel.
     */
    Executor executor();
}
