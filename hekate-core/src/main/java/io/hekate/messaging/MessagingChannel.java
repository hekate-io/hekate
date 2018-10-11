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
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.broadcast.Aggregate;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.Broadcast;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.loadbalance.DefaultLoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.Send;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.Subscribe;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.partition.PartitionMapper;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Messaging channel.
 *
 * <p>
 * This interface represents a channel for exchanging messages with remote nodes.
 * </p>
 *
 * <p>
 * Instances of this interface can be obtained via the {@link MessagingService#channel(String, Class)} method.
 * </p>
 *
 * <p>
 * For more details about messaging and channels please see the documentation of {@link MessagingService} interface.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by channels.
 *
 * @see MessagingService#channel(String, Class)
 */
public interface MessagingChannel<T> extends ClusterFilterSupport<MessagingChannel<T>> {
    /**
     * Creates a new {@link Send} operation.
     *
     * <p>
     * Send operation doesn't assume any response to be received from the destination node. If request-response type of communication is
     * required then consider using the {@link #newRequest(Object)} method.
     * </p>
     *
     * <p>
     * By default, this operation will not wait for the message to be processed on the receiver side. It is possible to change this behavior
     * via {@link Send#withConfirmReceive(boolean)} method.
     * </p>
     *
     * @param message Message to be sent.
     *
     * @return New operation.
     *
     * @see Send#submit()
     */
    Send<T> newSend(T message);

    /**
     * Creates a new {@link Request} operation.
     *
     * @param request Request.
     *
     * @return New operation.
     *
     * @see Request#submit()
     */
    Request<T> newRequest(T request);

    /**
     * Creates a new {@link Subscribe} operation.
     *
     * @param request Subscription request.
     *
     * @return New operation.
     *
     * @see Subscribe#submit(RequestCallback)
     */
    Subscribe<T> newSubscribe(T request);

    /**
     * Creates a new {@link Broadcast} operation.
     *
     * <p>
     * By default, this operation will not wait for the message to be processed on the receiver side. It is possible to change this behavior
     * via {@link Send#withConfirmReceive(boolean)} method.
     * </p>
     *
     * @param request Message to broadcast.
     *
     * @return New operation.
     *
     * @see Broadcast#submit()
     */
    Broadcast<T> newBroadcast(T request);

    /**
     * Creates a new {@link Aggregate} operation.
     *
     * @param request Aggregation request.
     *
     * @return New operation.
     *
     * @see Aggregate#submit()
     */
    Aggregate<T> newAggregate(T request);

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
     * Returns the partition mapper of this channel.
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
     * If not specified or set to {@code null} then {@link DefaultLoadBalancer} will be used.
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
     * Returns the asynchronous task executor of this channel.
     *
     * @return Asynchronous task executor of this channel.
     *
     * @see MessagingChannelConfig#setWorkerThreads(int)
     */
    Executor executor();

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
    default SendFuture send(T message) {
        return newSend(message).submit();
    }

    /**
     * Asynchronously broadcasts the specified message and returns a future object that can be used to inspect the operation result.
     *
     * @param message Message to broadcast.
     *
     * @return Future object that can be used to inspect the broadcast operation result.
     */
    default BroadcastFuture<T> broadcast(T message) {
        return newBroadcast(message).submit();
    }

    /**
     * Asynchronously sends the query message and aggregates responses from all the nodes that received this message. This method returns a
     * future object that can be used to inspect the aggregation results.
     *
     * @param message Query message that should be sent.
     *
     * @return Future object that can be used to inspect the aggregation results.
     */
    default AggregateFuture<T> aggregate(T message) {
        return newAggregate(message).submit();
    }

    /**
     * Asynchronously sends a request message and returns a future object that will be completed after receiving the response.
     *
     * @param request Request message.
     *
     * @return Future object that can be used to obtain the response.
     */
    default RequestFuture<T> request(T request) {
        return newRequest(request).submit();
    }

    /**
     * Opens a stream for receiving continuous responses.
     *
     * <p>
     * This method asynchronously sends a request message and opens a stream for receiving {@link Message#partialReply(Object) partial
     * replies}. For each such reply the {@link RequestCallback#onComplete(Throwable, Response)} method will be called until the last
     * (non-{@link Response#isPartial() partial}) response is received.
     * </p>
     *
     * @param request Request.
     * @param callback Callback.
     *
     * @return Future object that gets completed when the final chunk of a response stream is received.
     */
    default SubscribeFuture<T> subscribe(T request, RequestCallback<T> callback) {
        return newSubscribe(request).submit(callback);
    }
}
