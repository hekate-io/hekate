/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.core.HekateSupport;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
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
public interface MessagingChannel<T> extends ClusterFilterSupport<MessagingChannel<T>>, HekateSupport {
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
     * Returns the base type of messages that can be transferred through this channel.
     *
     * @return Base type of messages that can be transferred through this channel.
     *
     * @see MessagingChannelConfig#MessagingChannelConfig(Class)
     */
    Class<T> baseType();

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
     * <p>
     * By default, this operation will wait for the message to be processed on the receiver side. It is possible to disable this behavior
     * via {@link #withConfirmReceive(boolean)} method.
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
     * <p>
     * By default, this operation will wait for the message to be processed on the receiver side. It is possible to disable this behavior
     * via {@link #withConfirmReceive(boolean)} method.
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
     *
     * @return Future object that can be used to obtain the response.
     */
    ResponseFuture<T> request(T request);

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
     * <p>
     * By default, this operation will wait for the message to be processed on all receivers. It is possible to disable this behavior
     * via {@link #withConfirmReceive(boolean)} method.
     * </p>
     *
     * @param message Message to broadcast.
     *
     * @return Future object that can be used to inspect the broadcast operation result.
     */
    BroadcastFuture<T> broadcast(T message);

    /**
     * Asynchronously broadcasts the specified message and notifies the specified callback upon the operation completion.
     *
     * <p>
     * By default, this operation will wait for the message to be processed on all receivers. It is possible to disable this behavior
     * via {@link #withConfirmReceive(boolean)} method.
     * </p>
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
     *
     * @return Future object that can be used to inspect the aggregation results.
     *
     * @see #aggregate(Object, AggregateCallback)
     */
    AggregateFuture<T> aggregate(T message);

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
     * Specifying the affinity key ensures that all messages submitted with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * <p>
     * {@link #withLoadBalancer(LoadBalancer) Load balancer} can also make use of the affinity key to perform consistent routing of
     * messages among the cluster node. For example, the default load balancer makes sure that all messages, having the same key, are always
     * routed to the same node (unless the cluster topology doesn't change).
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
     * Returns the partition mapper that this channel uses to map {@link #withAffinity(Object) affinity keys} to the cluster nodes.
     *
     * @return Mapper.
     *
     * @see MessagingChannelConfig#setPartitions(int)
     * @see MessagingChannelConfig#setBackupNodes(int)
     * @see #withPartitions(int, int)
     */
    PartitionMapper partitions();

    /**
     * Returns a copy of this channel that will use a {@link PartitionMapper} with the specified options.
     *
     * @param partitions Total amount of partitions that should be managed by the channel's partition mapper.
     * @param backupNodes Amount of backup nodes that should be assigned to each partition by the the channel's partition mapper.
     *
     * @return Channel wrapper.
     *
     * @see #partitions()
     */
    MessagingChannel<T> withPartitions(int partitions, int backupNodes);

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
     * <p>
     * If not specified or set to {@code null} then the default load balancer will be used. Default load balancer uses
     * {@link RendezvousHashMapper} to route messages if {@link #withAffinity(Object) affinit key} is specified or uses random distribution
     * of messages among the cluster nodes if {@link #withAffinity(Object) affinity key} is not specified.
     * </p>
     *
     * @param balancer Load balancer.
     *
     * @return Channel wrapper.
     *
     * @see MessagingChannelConfig#setLoadBalancer(LoadBalancer)
     */
    MessagingChannel<T> withLoadBalancer(LoadBalancer<T> balancer);

    /**
     * Returns the cluster view of this channel.
     *
     * <p>
     * The returned cluster view contains only those nodes that have a {@link MessagingChannelConfig#setReceiver(MessageReceiver) receiver}
     * and do match the channel's {@link ClusterFilterSupport filtering} criteria.
     * </p>
     *
     * @return Cluster view.
     */
    ClusterView cluster();

    /**
     * Returns a copy of this channel that will use the specified cluster view and will inherit all other options from this instance.
     *
     * @param cluster Cluster view.
     *
     * @return Channel wrapper.
     */
    MessagingChannel<T> withCluster(ClusterView cluster);

    /**
     * Returns a copy of this channel that will use the specified confirmation mode and will inherit all other options from this instance.
     *
     * <p>
     * If this option is set to {@code true} then receiver of {@link #send(Object)} and {@link #broadcast(Object)} operation will send
     * back a confirmation to indicate that message was successfully {@link MessageReceiver#receive(Message) received}. In such case the
     * operation's callback/future will be notified only when such confirmation is received (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@code false} then operation will be assumed to be successful once the message gets flushed to the network
     * buffer without any additional confirmations from the receiver side.
     * </p>
     *
     * <p>
     * Default value of this option is {@code true} (i.e. confirmations are enabled by default).
     * </p>
     *
     * @param confirmReceive Confirmation mode.
     *
     * @return Channel wrapper.
     */
    MessagingChannel<T> withConfirmReceive(boolean confirmReceive);

    /**
     * Returns the confirmation mode of this channel.
     *
     * @return Confirmation mode of this channel.
     *
     * @see #withConfirmReceive(boolean)
     */
    boolean isConfirmReceive();

    /**
     * Returns the asynchronous task executor of this channel.
     *
     * @return Asynchronous task executor of this channel.
     *
     * @see MessagingChannelConfig#setWorkerThreads(int)
     */
    Executor executor();
}
