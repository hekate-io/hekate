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
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
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
    MessagingChannelId getId();

    /**
     * Returns the channel name.
     *
     * @return Channel name.
     *
     * @see MessagingChannelConfig#setName(String)
     */
    String getName();

    /**
     * Returns the cluster view of this channel.
     *
     * @return Cluster view.
     */
    ClusterView getCluster();

    /**
     * Returns the socket pool size (see {@link MessagingChannelConfig#setSockets(int)}).
     *
     * @return Socket pool size.
     */
    int getSockets();

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections
     * (see {@link MessagingChannelConfig#setNioThreads(int)}).
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    int getNioThreads();

    /**
     * Returns the worker thread pool size (see {@link MessagingChannelConfig#setWorkerThreads(int)}).
     *
     * @return Worker thread pool size.
     */
    int getWorkerThreads();

    /**
     * Asynchronously sends the one way message and returns a future object that can be used to inspect the operation result.
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
     * Asynchronously sends the one way message and notifies the specified callback upon the operation completion.
     *
     * <p>
     * This method doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #request(Object, RequestCallback)} method.
     * </p>
     *
     * @param message Message to be sent.
     * @param callback Callback to be notified upon the operation completion.
     */
    void send(T message, SendCallback callback);

    /**
     * Asynchronously sends the one way message and returns a future object that can be used to inspect the operation result.
     *
     * <p>
     * This method guarantees that all messages submitted with the same affinity key will always be processed by the same node (selected
     * from the channel's {@link #getCluster()} cluster topology)) unless topology doesn't change. Moreover it guarantees that the target
     * node will always use the same thread to process such messages.
     * </p>
     *
     * <p>
     * This method doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #request(Object)} method.
     * </p>
     *
     * @param affinityKey Message affinity key. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Message to be sent.
     *
     * @return Future object that can be used to inspect the operation result.
     */
    SendFuture affinitySend(Object affinityKey, T message);

    /**
     * Asynchronously sends the one way message and notifies the specified callback upon the operation completion.
     *
     * <p>
     * This method guarantees that all messages submitted with the same affinity key will always be processed by the same node (selected
     * from the channel's {@link #getCluster()} cluster topology)) unless topology doesn't change. Moreover it guarantees that the target
     * node will always use the same thread to process such messages.
     * </p>
     *
     * <p>
     * This method doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #request(Object, RequestCallback)} method.
     * </p>
     *
     * @param affinityKey Message affinity key. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Message to be sent.
     * @param callback Callback to be notified upon the operation completion.
     */
    void affinitySend(Object affinityKey, T message, SendCallback callback);

    /**
     * Asynchronously sends the request message and awaits for response from a destination node. This method returns a future object
     * that can be used to obtain the response.
     *
     * @param message Request message.
     * @param <R> Expected response type.
     *
     * @return Future object that can be used to obtain the response.
     */
    <R extends T> RequestFuture<R> request(T message);

    /**
     * Asynchronously sends the request message, awaits for response from a destination node and notifies the specified callback.
     *
     * @param message Request message.
     * @param callback Callback to be notified upon the operation completion.
     */
    void request(T message, RequestCallback<T> callback);

    /**
     * Asynchronously sends the request message and awaits for response from a destination node. This method returns a future object
     * that can be used to obtain the response.
     *
     * <p>
     * This method guarantees that all requests submitted with the same affinity key will always be processed by the same node (selected
     * from the channel's {@link #getCluster()} cluster topology)) unless topology doesn't change. Moreover it guarantees that the target
     * node will always use the same thread to process such requests.
     * </p>
     *
     * @param affinityKey Message affinity key. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Request message.
     * @param <R> Expected response type.
     *
     * @return Future object that can be used to obtain the response.
     */
    <R extends T> RequestFuture<R> affinityRequest(Object affinityKey, T message);

    /**
     * Asynchronously sends the request message, awaits for response from a destination node and notifies the specified callback.
     *
     * <p>
     * This method guarantees that all requests submitted with the same affinity key will always be processed by the same node (selected
     * from the channel's {@link #getCluster()} cluster topology)) unless topology doesn't change. Moreover it guarantees that the target
     * node will always use the same thread to process such requests.
     * </p>
     *
     * @param affinityKey Message affinity key. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Request message.
     * @param callback Callback to be notified upon the operation completion.
     */
    void affinityRequest(Object affinityKey, T message, RequestCallback<T> callback);

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
     * Asynchronously broadcasts the specified message and returns a future object that can be used to inspect the operation result.
     *
     * @param affinityKey Message affinity key to make sure that messages with the same key will always go through the same network
     * connection and will be processed by the same thread. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Message to broadcast.
     *
     * @return Future object that can be used to inspect the broadcast operation result.
     */
    BroadcastFuture<T> affinityBroadcast(Object affinityKey, T message);

    /**
     * Asynchronously broadcasts the specified message and notifies the specified callback upon the operation completion.
     *
     * @param affinityKey Message affinity key to make sure that messages with the same key will always go through the same network
     * connection and will be processed by the same thread. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Message to broadcast.
     * @param callback Callback that should be notified upon the broadcast operation completion.
     */
    void affinityBroadcast(Object affinityKey, T message, BroadcastCallback<T> callback);

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
     * Asynchronously sends the query message and aggregates responses from all the nodes that received this message. This method returns a
     * future object that can be used to inspect the aggregation results.
     *
     * @param affinityKey Message affinity key to make sure that messages with the same key will always go through the same network
     * connection and will be processed by the same thread. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Query message that should be sent.
     * @param <R> Response type (each remote node should return a response of this type).
     *
     * @return Future object that can be used to inspect the aggregation results.
     *
     * @see #aggregate(Object, AggregateCallback)
     */
    <R extends T> AggregateFuture<R> affinityAggregate(Object affinityKey, T message);

    /**
     * Asynchronously sends the query message, aggregates responses from all nodes that received this message and notifies the
     * specified callback on operation progress and aggregation results.
     *
     * @param affinityKey Message affinity key to make sure that messages with the same key will always go through the same network
     * connection and will be processed by the same thread. Note that if affinity key is defined at the channel level
     * (see {@link #withAffinityKey(Object)}) then such key will be completely ignored and only the key that is provided as parameter of
     * this method will be used for this operation.
     * @param message Query message that should be sent.
     * @param callback Callback that should be notified on operation progress and aggregation results.
     *
     * @see #aggregate(Object)
     */
    void affinityAggregate(Object affinityKey, T message, AggregateCallback<T> callback);

    /**
     * Returns a new lightweight wrapper of this channel that will use the specified affinity key and will inherit all other options from
     * this instance.
     *
     * <p>
     * The specified affinity key will be used by default for methods that do not provide their own affinity keys
     * (f.e. {@link #send(Object)}, {@link #request(Object)}, etc).
     * </p>
     *
     * <p>
     * It is possible to provide an affinity key for each operation individually by using a method that directly accept affinity key as
     * parameter (f.e. {@link #affinitySend(Object, Object)}, {@link #affinityRequest(Object, Object)}, etc). Affinity keys of such methods
     * have higher precedence over the affinity key of this method.
     * </p>
     *
     * @param affinityKey Affinity key.
     * @param <C> Channel type.
     *
     * @return Channel wrapper.
     */
    <C extends T> MessagingChannel<C> withAffinityKey(Object affinityKey);

    /**
     * Returns a new lightweight wrapper of this channel that will use the specified failover policy and will inherit all other options from
     * this instance.
     *
     * @param policy Failover policy.
     * @param <C> Channel type.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setFailoverPolicy(FailoverPolicy)
     */
    <C extends T> MessagingChannel<C> withFailover(FailoverPolicy policy);

    /**
     * Returns a new lightweight wrapper of this channel that will use the specified failover policy and will inherit all other options from
     * this instance.
     *
     * @param policy Failover policy.
     * @param <C> Channel type.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setFailoverPolicy(FailoverPolicy)
     */
    <C extends T> MessagingChannel<C> withFailover(FailoverPolicyBuilder policy);

    /**
     * Returns a new lightweight wrapper of this channel that will use the specified timeout value and will inherit all other options from
     * this instance.
     *
     * <p>
     * If particular messaging operation (f.e. {@link MessagingChannel#request(Object) request(...)}
     * or {@link MessagingChannel#send(Object) send(...)}) can't be completed within the specified timeout then such operation will fail
     * with {@link MessageTimeoutException}.
     * </p>
     *
     * <p>
     * Specifying a negative or a zero value will disable timeout checking.
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
     * Returns a new lightweight wrapper of this channel that will use the specified load balancer and will inherit all other options from
     * this instance.
     *
     * @param balancer Load balancer.
     * @param <C> Channel type.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setLoadBalancer(LoadBalancer)
     */
    <C extends T> MessagingChannel<C> withLoadBalancer(LoadBalancer<C> balancer);

    /**
     * Returns the asynchronous task executor of this channel.
     *
     * <p>
     * The returned executor can be a single-thread or a wrapper over the multi-threaded executor depending on {@link
     * MessagingChannelConfig#setWorkerThreads(int)} value.
     * </p>
     *
     * @return Asynchronous task executor of this channel.
     */
    Executor getExecutor();

    /**
     * Returned the asynchronous single-threaded task executor for the specified affinity key.
     *
     * @param affinityKey Affinity key.
     *
     * @return Asynchronous single-threaded task executor for the specified affinity key.
     */
    Executor getExecutor(Object affinityKey);
}
