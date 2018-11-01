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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterView;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.BackoffPolicy;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.broadcast.Aggregate;
import io.hekate.messaging.broadcast.Broadcast;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.Send;
import io.hekate.messaging.unicast.Subscribe;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessageOperationOpts<T> {
    private final MessagingGateway<T> gateway;

    @ToStringIgnore
    private final ClusterView cluster;

    private final RendezvousHashMapper partitions;

    private final FailoverPolicy failover;

    private final BackoffPolicy backoff;

    private final LoadBalancer<T> balancer;

    private final long timeout;

    public DefaultMessagingChannel(
        MessagingGateway<T> gateway,
        ClusterView cluster,
        RendezvousHashMapper partitions,
        LoadBalancer<T> balancer,
        FailoverPolicy failover,
        BackoffPolicy backoff,
        long timeout
    ) {
        assert gateway != null : "Gateway is null.";
        assert cluster != null : "Cluster view is null.";
        assert partitions != null : "Partition mapper is null.";
        assert backoff != null : "Backoff policy is null.";

        this.gateway = gateway;
        this.cluster = cluster;
        this.partitions = partitions;
        this.balancer = balancer;
        this.failover = failover;
        this.backoff = backoff;
        this.timeout = timeout;
    }

    @Override
    public Send<T> newSend(T message) {
        return new SendOperationBuilder<>(message, context(), this);
    }

    @Override
    public Request<T> newRequest(T request) {
        return new RequestOperationBuilder<>(request, context(), this);
    }

    @Override
    public Subscribe<T> newSubscribe(T request) {
        return new SubscribeOperationBuilder<>(request, context(), this);
    }

    @Override
    public Broadcast<T> newBroadcast(T request) {
        return new BroadcastOperationBuilder<>(request, context(), this);
    }

    @Override
    public Aggregate<T> newAggregate(T request) {
        return new AggregateOperationBuilder<>(request, context(), this);
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
            balancer,
            failover,
            backoff,
            timeout
        );
    }

    @Override
    public DefaultMessagingChannel<T> withLoadBalancer(LoadBalancer<T> balancer) {
        ArgAssert.notNull(balancer, "balancer");

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            failover,
            backoff,
            timeout
        );
    }

    @Override
    public Executor executor() {
        return gateway.executor();
    }

    @Override
    public DefaultMessagingChannel<T> withFailover(FailoverPolicy policy) {
        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            policy,
            backoff,
            timeout
        );
    }

    @Override
    public DefaultMessagingChannel<T> withFailover(FailoverPolicyBuilder policy) {
        return withFailover(policy.build());
    }

    @Override
    public DefaultMessagingChannel<T> withTimeout(long timeout, TimeUnit unit) {
        ArgAssert.notNull(unit, "Time unit");

        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            failover,
            backoff,
            unit.toMillis(timeout)
        );
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
            balancer,
            failover,
            backoff,
            timeout
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
            balancer,
            failover,
            backoff,
            timeout
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

    @Override
    public FailoverPolicy failover() {
        return failover;
    }

    @Override
    public MessagingChannel<T> withBackoff(BackoffPolicy backoff) {
        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            failover,
            backoff,
            timeout
        );
    }

    @Override
    public BackoffPolicy backoff() {
        return backoff;
    }

    @Override
    public long timeout() {
        return timeout;
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
