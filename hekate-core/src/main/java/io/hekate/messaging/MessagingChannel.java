/*
 * Copyright 2019 The Hekate Project
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
import io.hekate.messaging.loadbalance.DefaultLoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.AggregateResult;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.BroadcastResult;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.RequestFuture;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.operation.Subscribe;
import io.hekate.messaging.operation.SubscribeCallback;
import io.hekate.messaging.operation.SubscribeFuture;
import io.hekate.partition.PartitionMapper;
import java.util.List;
import java.util.concurrent.Executor;

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
     * via {@link Send#withAckMode(AckMode)} method.
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
     * @see Subscribe#submit(SubscribeCallback)
     */
    Subscribe<T> newSubscribe(T request);

    /**
     * Creates a new {@link Broadcast} operation.
     *
     * <p>
     * By default, this operation will not wait for the message to be processed on the receiver side. It is possible to change this behavior
     * via {@link Broadcast#withAckMode(AckMode)} method.
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
     * Returns the executor of this channel.
     *
     * @return Asynchronous task executor of this channel.
     *
     * @see MessagingChannelConfig#setWorkerThreads(int)
     */
    Executor executor();

    /**
     * Asynchronously sends the specified message with a mandatory acknowledgement.
     *
     * @param msg Message.
     *
     * @return Future object to be completed after receiving an acknowledgement.
     */
    default SendFuture sendAsync(T msg) {
        return newSend(msg)
            .withAckMode(AckMode.REQUIRED)
            .submit();
    }

    /**
     * Asynchronously sends the specified message with the specified acknowledgement mode.
     *
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Operation future.
     */
    default SendFuture sendAsync(T msg, AckMode ackMode) {
        return newSend(msg)
            .withAckMode(ackMode)
            .submit();
    }

    /**
     * Asynchronously sends the specified message with a mandatory acknowledgement.
     *
     * @param affinityKey Affinity key (see {@link Send#withAffinity(Object)}).
     * @param msg Message.
     *
     * @return Future object to be completed after receiving an acknowledgement.
     */
    default SendFuture sendAsync(Object affinityKey, T msg) {
        return newSend(msg)
            .withAckMode(AckMode.REQUIRED)
            .withAffinity(affinityKey)
            .submit();
    }

    /**
     * Asynchronously sends the specified message with the specified acknowledgement mode.
     *
     * @param affinityKey Affinity key (see {@link Send#withAffinity(Object)}).
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Operation future.
     */
    default SendFuture sendAsync(Object affinityKey, T msg, AckMode ackMode) {
        return newSend(msg)
            .withAckMode(ackMode)
            .withAffinity(affinityKey)
            .submit();
    }

    /**
     * Synchronously sends the specified message and awaits for an acknowledgement.
     *
     * @param msg Message.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default void send(T msg) throws MessagingFutureException, InterruptedException {
        newSend(msg)
            .withAckMode(AckMode.REQUIRED)
            .sync();
    }

    /**
     * Synchronously sends the specified message with the specified acknowledgement mode.
     *
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default void send(T msg, AckMode ackMode) throws MessagingFutureException, InterruptedException {
        newSend(msg)
            .withAckMode(ackMode)
            .sync();
    }

    /**
     * Synchronously sends the specified message and awaits for an acknowledgement.
     *
     * @param affinityKey Affinity key (see {@link Send#withAffinity(Object)}).
     * @param msg Message.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default void send(Object affinityKey, T msg) throws MessagingFutureException, InterruptedException {
        newSend(msg)
            .withAckMode(AckMode.REQUIRED)
            .withAffinity(affinityKey)
            .sync();
    }

    /**
     * Synchronously sends the specified message with the specified acknowledgement mode.
     *
     * @param affinityKey Affinity key (see {@link Send#withAffinity(Object)}).
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default void send(Object affinityKey, T msg, AckMode ackMode) throws MessagingFutureException, InterruptedException {
        newSend(msg)
            .withAckMode(ackMode)
            .withAffinity(affinityKey)
            .sync();
    }

    /**
     * Asynchronously sends the specified request.
     *
     * @param msg Request message.
     *
     * @return Future object to be completed after receiving a response.
     */
    default RequestFuture<T> requestAsync(T msg) {
        return newRequest(msg).submit();
    }

    /**
     * Asynchronously sends the specified request.
     *
     * @param affinityKey Affinity key (see {@link Request#withAffinity(Object)}).
     * @param msg Request message.
     *
     * @return Future object to be completed after receiving a response.
     */
    default RequestFuture<T> requestAsync(Object affinityKey, T msg) {
        return newRequest(msg)
            .withAffinity(affinityKey)
            .submit();
    }

    /**
     * Synchronously sends the specified request and awaits for a response.
     *
     * @param msg Request message.
     *
     * @return Response.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default T request(T msg) throws MessagingFutureException, InterruptedException {
        return newRequest(msg).response();
    }

    /**
     * Synchronously sends the specified request and awaits for a response.
     *
     * @param affinityKey Affinity key (see {@link Request#withAffinity(Object)}).
     * @param msg Request message.
     *
     * @return Response.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default T request(Object affinityKey, T msg) throws MessagingFutureException, InterruptedException {
        return newRequest(msg)
            .withAffinity(affinityKey)
            .response();
    }

    /**
     * Asynchronously sends the specified subscription request.
     *
     * @param msg Subscription request message.
     * @param callback Subscription callback.
     *
     * @return Future object to be completed after receiving the final response (see {@link ResponsePart#isLastPart()}).
     */
    default SubscribeFuture<T> subscribeAsync(T msg, SubscribeCallback<T> callback) {
        return newSubscribe(msg).submit(callback);
    }

    /**
     * Asynchronously sends the specified subscription request.
     *
     * @param affinityKey Affinity key (see {@link Subscribe#withAffinity(Object)}).
     * @param msg Subscription request message.
     * @param callback Subscription callback.
     *
     * @return Future object to be completed after receiving the final response (see {@link ResponsePart#isLastPart()}).
     */
    default SubscribeFuture<T> subscribeAsync(Object affinityKey, T msg, SubscribeCallback<T> callback) {
        return newSubscribe(msg)
            .withAffinity(affinityKey)
            .submit(callback);
    }

    /**
     * Synchronously sends the specified subscription request and accumulates all responses.
     *
     * <p>
     * <b>Notice:</b> This method blocks until the final response is received (see {@link ResponsePart#isLastPart()}) and accumulates all
     * intermediate parts in memory.
     * </p>
     *
     * @param msg Subscription request message.
     *
     * @return All response parts that were received.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default List<T> subscribe(T msg) throws MessagingFutureException, InterruptedException {
        return newSubscribe(msg).responses();
    }

    /**
     * Synchronously sends the specified subscription request and accumulates all responses.
     *
     * <p>
     * <b>Notice:</b> This method blocks until the final response is received (see {@link ResponsePart#isLastPart()}) and accumulates all
     * intermediate parts in memory.
     * </p>
     *
     * @param affinityKey Affinity key (see {@link Subscribe#withAffinity(Object)}).
     * @param msg Subscription request message.
     *
     * @return All response parts that were received.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default List<T> subscribe(Object affinityKey, T msg) throws MessagingFutureException, InterruptedException {
        return newSubscribe(msg)
            .withAffinity(affinityKey)
            .responses();
    }

    /**
     * Asynchronously broadcasts the specified message with a mandatory acknowledgement.
     *
     * @param msg Message.
     *
     * @return Future object to be completed after receiving acknowledgements from all nodes.
     */
    default BroadcastFuture<T> broadcastAsync(T msg) {
        return newBroadcast(msg)
            .withAckMode(AckMode.REQUIRED)
            .submit();
    }

    /**
     * Asynchronously broadcasts the specified message with the specified acknowledgement mode.
     *
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Operation future.
     */
    default BroadcastFuture<T> broadcastAsync(T msg, AckMode ackMode) {
        return newBroadcast(msg)
            .withAckMode(ackMode)
            .submit();
    }

    /**
     * Asynchronously broadcasts the specified message with a mandatory acknowledgement.
     *
     * @param affinityKey Affinity key (see {@link Broadcast#withAffinity(Object)}).
     * @param msg Message.
     *
     * @return Future object to be completed after receiving acknowledgements from all nodes.
     */
    default BroadcastFuture<T> broadcastAsync(Object affinityKey, T msg) {
        return newBroadcast(msg)
            .withAckMode(AckMode.REQUIRED)
            .withAffinity(affinityKey)
            .submit();
    }

    /**
     * Asynchronously broadcasts the specified message with the specified acknowledgement mode.
     *
     * @param affinityKey Affinity key (see {@link Broadcast#withAffinity(Object)}).
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Operation future.
     */
    default BroadcastFuture<T> broadcastAsync(Object affinityKey, T msg, AckMode ackMode) {
        return newBroadcast(msg)
            .withAckMode(ackMode)
            .withAffinity(affinityKey)
            .submit();
    }

    /**
     * Synchronously broadcasts the specified message with a mandatory acknowledgement.
     *
     * @param msg Message.
     *
     * @return Broadcast result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default BroadcastResult<T> broadcast(T msg) throws MessagingFutureException, InterruptedException {
        return newBroadcast(msg)
            .withAckMode(AckMode.REQUIRED)
            .sync();
    }

    /**
     * Synchronously broadcasts the specified message with the specified acknowledgement mode.
     *
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Broadcast result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default BroadcastResult<T> broadcast(T msg, AckMode ackMode) throws MessagingFutureException, InterruptedException {
        return newBroadcast(msg)
            .withAckMode(ackMode)
            .sync();
    }

    /**
     * Synchronously broadcasts the specified message with a mandatory acknowledgement.
     *
     * @param affinityKey Affinity key (see {@link Broadcast#withAffinity(Object)}).
     * @param msg Message.
     *
     * @return Broadcast result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default BroadcastResult<T> broadcast(Object affinityKey, T msg) throws MessagingFutureException, InterruptedException {
        return newBroadcast(msg)
            .withAckMode(AckMode.REQUIRED)
            .withAffinity(affinityKey).sync();
    }

    /**
     * Synchronously broadcasts the specified message with the specified acknowledgement mode.
     *
     * @param affinityKey Affinity key (see {@link Broadcast#withAffinity(Object)}).
     * @param msg Message.
     * @param ackMode Acknowledgement mode.
     *
     * @return Broadcast result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default BroadcastResult<T> broadcast(Object affinityKey, T msg, AckMode ackMode) throws MessagingFutureException, InterruptedException {
        return newBroadcast(msg)
            .withAckMode(ackMode)
            .withAffinity(affinityKey)
            .sync();
    }

    /**
     * Asynchronously submits the specified aggregation request.
     *
     * @param msg Aggregation request message.
     *
     * @return Future object to be completed after receiving responses from all nodes.
     */
    default AggregateFuture<T> aggregateAsync(T msg) {
        return newAggregate(msg).submit();
    }

    /**
     * Asynchronously submits the specified aggregation request.
     *
     * @param affinityKey Affinity key (see {@link Aggregate#withAffinity(Object)}).
     * @param msg Aggregation request message.
     *
     * @return Future object to be completed after receiving responses from all nodes.
     */
    default AggregateFuture<T> aggregateAsync(Object affinityKey, T msg) {
        return newAggregate(msg).withAffinity(affinityKey).submit();
    }

    /**
     * Synchronously submits the specified aggregation request.
     *
     * @param msg Aggregation request message.
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default AggregateResult<T> aggregate(T msg) throws MessagingFutureException, InterruptedException {
        return newAggregate(msg).get();
    }

    /**
     * Synchronously submits the specified aggregation request.
     *
     * @param affinityKey Affinity key (see {@link Aggregate#withAffinity(Object)}).
     * @param msg Aggregation request message.
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException If message operation failed.
     * @throws InterruptedException If current thread got interrupted.
     */
    default AggregateResult<T> aggregate(Object affinityKey, T msg) throws MessagingFutureException, InterruptedException {
        return newAggregate(msg).withAffinity(affinityKey).get();
    }
}
