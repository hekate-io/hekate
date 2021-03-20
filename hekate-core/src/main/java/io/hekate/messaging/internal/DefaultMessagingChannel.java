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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.internal.TopologyContextCache;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.Subscribe;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;

/**
 * Default implementation of {@link MessagingChannel} interface.
 *
 * @param <T> Message type.
 */
class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessageOperationOpts<T> {
    /** Messaging gateway. */
    private final MessagingGateway<T> gateway;

    /** Cluster view of this channel. */
    @ToStringIgnore
    private final ClusterView cluster;

    /** Partition mapper (see {@link #withPartitions(PartitionMapper)}). */
    private final PartitionMapper partitions;

    /** Load balancer (see {@link #withLoadBalancer(LoadBalancer)}. */
    private final LoadBalancer<T> balancer;

    /** Cache for load balancer. */
    @ToStringIgnore
    private TopologyContextCache balancerCache;

    /**
     * Constructs a new instance.
     *
     * @param gateway Messaging gateway.
     * @param cluster Cluster view of this channel.
     * @param partitions Partition mapper.
     * @param balancer Load balancer.
     */
    public DefaultMessagingChannel(
        MessagingGateway<T> gateway,
        ClusterView cluster,
        PartitionMapper partitions,
        LoadBalancer<T> balancer
    ) {
        this.gateway = gateway;
        this.cluster = cluster;
        this.partitions = partitions;
        this.balancer = balancer;
    }

    @Override
    public Send<T> newSend(T message) {
        SendOperationBuilder<T> builder = new SendOperationBuilder<>(message, context(), this);

        gateway.baseRetryPolicy().configure(builder);

        return builder;
    }

    @Override
    public Request<T> newRequest(T request) {
        RequestOperationBuilder<T> builder = new RequestOperationBuilder<>(request, context(), this);

        gateway.baseRetryPolicy().configure(builder);

        return builder;
    }

    @Override
    public Subscribe<T> newSubscribe(T request) {
        SubscribeOperationBuilder<T> builder = new SubscribeOperationBuilder<>(request, context(), this);

        gateway.baseRetryPolicy().configure(builder);

        return builder;
    }

    @Override
    public Broadcast<T> newBroadcast(T request) {
        BroadcastOperationBuilder<T> builder = new BroadcastOperationBuilder<>(request, context(), this);

        gateway.baseRetryPolicy().configure(builder);

        return builder;
    }

    @Override
    public Aggregate<T> newAggregate(T request) {
        AggregateOperationBuilder<T> builder = new AggregateOperationBuilder<>(request, context(), this);

        gateway.baseRetryPolicy().configure(builder);

        return builder;
    }

    @Override
    public MessagingChannelId id() {
        return gateway.channelId();
    }

    @Override
    public String name() {
        return gateway.name();
    }

    @Override
    public Class<T> baseType() {
        return gateway.baseType();
    }

    @Override
    public PartitionMapper partitions() {
        return partitions;
    }

    @Override
    public TopologyContextCache balancerCache() {
        // No synchronization here.
        // It is ok if different threads will construct and access different cache instances in parallel.
        if (balancerCache == null) {
            balancerCache = new TopologyContextCache();
        }

        return balancerCache;
    }

    @Override
    public DefaultMessagingChannel<T> withPartitions(int partitions, int backupNodes) {
        if (partitions().partitions() == partitions && partitions().backupNodes() == backupNodes) {
            return this;
        }

        RendezvousHashMapper mapper = RendezvousHashMapper.of(cluster, partitions, backupNodes);

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            mapper,
            balancer
        );
    }

    @Override
    public MessagingChannel<T> withPartitions(PartitionMapper mapper) {
        ArgAssert.notNull(mapper, "Mapper");

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            mapper,
            balancer
        );
    }

    @Override
    public DefaultMessagingChannel<T> withLoadBalancer(LoadBalancer<T> balancer) {
        ArgAssert.notNull(balancer, "Load balancer");

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer
        );
    }

    @Override
    public Executor executor() {
        return gateway.executor();
    }

    @Override
    public DefaultMessagingChannel<T> filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        ClusterView newCluster = cluster.filterAll(filter);
        PartitionMapper newPartitions = partitions.copy(newCluster);

        return new DefaultMessagingChannel<>(
            gateway,
            newCluster,
            newPartitions,
            balancer
        );
    }

    @Override
    public ClusterView cluster() {
        return cluster;
    }

    @Override
    public DefaultMessagingChannel<T> withCluster(ClusterView cluster) {
        ArgAssert.notNull(cluster, "Cluster");

        ClusterView newCluster = cluster.filter(MessagingMetaData.hasReceiver(gateway.name()));
        PartitionMapper newPartitions = partitions.copy(newCluster);

        return new DefaultMessagingChannel<>(
            gateway,
            newCluster,
            newPartitions,
            balancer
        );
    }

    @Override
    public int nioThreads() {
        return gateway.nioThreads();
    }

    @Override
    public int workerThreads() {
        return gateway.workerThreads();
    }

    @Override
    public LoadBalancer<T> balancer() {
        return balancer;
    }

    // Package level for testing purposes.
    MessagingGatewayContext<T> context() {
        return gateway.requireContext();
    }

    @Override
    public String toString() {
        return ToString.format(MessagingChannel.class, this);
    }
}
