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
import io.hekate.core.Hekate;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessagingOpts<T> {
    private static final boolean DEFAULT_CONFIRM_RECEIVE = true;

    private final MessagingGateway<T> gateway;

    @ToStringIgnore
    private final ClusterView cluster;

    private final RendezvousHashMapper partitions;

    private final FailoverPolicy failover;

    private final LoadBalancer<T> balancer;

    private final Object affinityKey;

    private final long timeout;

    private final boolean confirmReceive;

    public DefaultMessagingChannel(
        MessagingGateway<T> gateway,
        ClusterView cluster,
        RendezvousHashMapper partitions,
        LoadBalancer<T> balancer,
        FailoverPolicy failover,
        long timeout
    ) {
        this(gateway, cluster, partitions, balancer, failover, timeout, DEFAULT_CONFIRM_RECEIVE, null);
    }

    private DefaultMessagingChannel(
        MessagingGateway<T> gateway,
        ClusterView cluster,
        RendezvousHashMapper partitions,
        LoadBalancer<T> balancer,
        FailoverPolicy failover,
        long timeout,
        boolean confirmReceive,
        Object affinityKey
    ) {
        assert gateway != null : "Gateway is null.";
        assert cluster != null : "Cluster view is null.";
        assert partitions != null : "Partition mapper is null.";

        this.gateway = gateway;
        this.cluster = cluster;
        this.partitions = partitions;
        this.balancer = balancer;
        this.failover = failover;
        this.timeout = timeout;
        this.confirmReceive = confirmReceive;
        this.affinityKey = affinityKey;
    }

    @Override
    public SendFuture send(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.send(affinityKey, message, this);
    }

    @Override
    public void send(T message, SendCallback callback) {
        ArgAssert.notNull(message, "Message");

        gateway.send(affinityKey, message, this, callback);
    }

    @Override
    public ResponseFuture<T> request(T request) {
        ArgAssert.notNull(request, "Message");

        return gateway.request(affinityKey, request, this);
    }

    @Override
    public void request(T request, ResponseCallback<T> callback) {
        ArgAssert.notNull(request, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.request(affinityKey, request, this, callback);
    }

    @Override
    public void subscribe(T request, ResponseCallback<T> callback) {
        ArgAssert.notNull(request, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.subscribe(affinityKey, request, this, callback);
    }

    @Override
    public SubscribeFuture<T> subscribe(T request) {
        ArgAssert.notNull(request, "Message");

        return gateway.subscribe(affinityKey, request, this);
    }

    @Override
    public BroadcastFuture<T> broadcast(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.broadcast(affinityKey, message, this);
    }

    @Override
    public void broadcast(T message, BroadcastCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.broadcast(affinityKey, message, this, callback);
    }

    @Override
    public AggregateFuture<T> aggregate(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.aggregate(affinityKey, message, this);
    }

    @Override
    public void aggregate(T message, AggregateCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.aggregate(affinityKey, message, this, callback);
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
    public DefaultMessagingChannel<T> withAffinity(Object affinityKey) {
        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            failover,
            timeout,
            confirmReceive,
            affinityKey
        );
    }

    @Override
    public Object affinity() {
        return affinityKey;
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
            timeout,
            confirmReceive,
            affinityKey
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
            timeout,
            confirmReceive,
            affinityKey
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
            timeout,
            confirmReceive,
            affinityKey
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
            unit.toMillis(timeout),
            confirmReceive,
            affinityKey
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
            timeout,
            confirmReceive,
            affinityKey
        );
    }

    @Override
    public ClusterView cluster() {
        return cluster;
    }

    @Override
    public DefaultMessagingChannel<T> withCluster(ClusterView cluster) {
        ArgAssert.notNull(cluster, "Cluster");

        ClusterView newCluster = cluster.filter(ChannelMetaData.hasReceiver(gateway.name()));
        RendezvousHashMapper newPartitions = partitions.copy(newCluster);

        return new DefaultMessagingChannel<>(
            gateway,
            newCluster,
            newPartitions,
            balancer,
            failover,
            timeout,
            confirmReceive,
            affinityKey
        );
    }

    @Override
    public DefaultMessagingChannel<T> withConfirmReceive(boolean confirmReceive) {
        return new DefaultMessagingChannel<>(
            gateway,
            cluster,
            partitions,
            balancer,
            failover,
            timeout,
            confirmReceive,
            affinityKey
        );
    }

    @Override
    public boolean isConfirmReceive() {
        return confirmReceive;
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
    public long timeout() {
        return timeout;
    }

    @Override
    public Hekate hekate() {
        return gateway.hekate();
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
