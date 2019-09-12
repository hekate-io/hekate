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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterView;
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

class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessageOperationOpts<T> {
    private final MessagingGateway<T> gateway;

    @ToStringIgnore
    private final ClusterView cluster;

    private final RendezvousHashMapper partitions;

    private final LoadBalancer<T> balancer;

    public DefaultMessagingChannel(
        MessagingGateway<T> gateway,
        ClusterView cluster,
        RendezvousHashMapper partitions,
        LoadBalancer<T> balancer
    ) {
        assert gateway != null : "Gateway is null.";
        assert cluster != null : "Cluster view is null.";
        assert partitions != null : "Partition mapper is null.";

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
    public DefaultMessagingChannel<T> withPartitions(int partitions, int backupNodes) {
        if (partitions().partitions() == partitions && partitions().backupNodes() == backupNodes) {
            return this;
        }

        RendezvousHashMapper newPartitions = RendezvousHashMapper.of(cluster, partitions, backupNodes);

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            newPartitions,
            balancer
        );
    }

    @Override
    public DefaultMessagingChannel<T> withLoadBalancer(LoadBalancer<T> balancer) {
        ArgAssert.notNull(balancer, "balancer");

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
        RendezvousHashMapper newPartitions = partitions.copy(newCluster);

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
        RendezvousHashMapper newPartitions = partitions.copy(newCluster);

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
