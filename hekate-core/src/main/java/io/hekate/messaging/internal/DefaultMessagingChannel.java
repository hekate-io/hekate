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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterView;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessagingOpts<T> {
    private final MessagingGateway<T> gateway;

    @ToStringIgnore
    private final ClusterView cluster;

    private final Object affinity;

    private final FailoverPolicy failover;

    private final LoadBalancer<T> balancer;

    private final long timeout;

    public DefaultMessagingChannel(MessagingGateway<T> gateway, ClusterView cluster, LoadBalancer<T> balancer, FailoverPolicy failover,
        Object affinity, long timeout) {
        assert gateway != null : "Gateway is null.";
        assert cluster != null : "Cluster view is null.";

        this.gateway = gateway;
        this.cluster = cluster;
        this.balancer = balancer;
        this.failover = failover;
        this.affinity = affinity;
        this.timeout = timeout;
    }

    @Override
    public SendFuture send(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.send(affinity, message, this);
    }

    @Override
    public void send(T message, SendCallback callback) {
        ArgAssert.notNull(message, "Message");

        gateway.send(affinity, message, this, callback);
    }

    @Override
    public SendFuture affinitySend(Object affinityKey, T message) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(affinityKey, "Affinity key");

        return gateway.send(affinityKey, message, this);
    }

    @Override
    public void affinitySend(Object affinityKey, T message, SendCallback callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");
        ArgAssert.notNull(affinityKey, "Affinity key");

        gateway.send(affinityKey, message, this, callback);
    }

    @Override
    public <R extends T> RequestFuture<R> request(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.request(affinity, message, this);
    }

    @Override
    public void request(T message, RequestCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.request(affinity, message, this, callback);
    }

    @Override
    public <R extends T> RequestFuture<R> affinityRequest(Object affinityKey, T message) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(affinityKey, "Affinity key");

        return gateway.request(affinityKey, message, this);
    }

    @Override
    public void affinityRequest(Object affinityKey, T message, RequestCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");
        ArgAssert.notNull(affinityKey, "Affinity key");

        gateway.request(affinityKey, message, this, callback);
    }

    @Override
    public BroadcastFuture<T> broadcast(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.broadcast(affinity, message, cluster);
    }

    @Override
    public void broadcast(T message, BroadcastCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.broadcast(affinity, message, cluster, callback);
    }

    @Override
    public BroadcastFuture<T> affinityBroadcast(Object affinityKey, T message) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(affinityKey, "Affinity key");

        return gateway.broadcast(affinityKey, message, cluster);
    }

    @Override
    public void affinityBroadcast(Object affinityKey, T message, BroadcastCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");
        ArgAssert.notNull(affinityKey, "Affinity key");

        gateway.broadcast(affinityKey, message, cluster, callback);
    }

    @Override
    public <R extends T> AggregateFuture<R> aggregate(T message) {
        ArgAssert.notNull(message, "Message");

        return gateway.aggregate(affinity, message, cluster);
    }

    @Override
    public void aggregate(T message, AggregateCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");

        gateway.aggregate(affinity, message, cluster, callback);
    }

    @Override
    public <R extends T> AggregateFuture<R> affinityAggregate(Object affinityKey, T message) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(affinityKey, "Affinity key");

        return gateway.aggregate(affinityKey, message, cluster);
    }

    @Override
    public void affinityAggregate(Object affinityKey, T message, AggregateCallback<T> callback) {
        ArgAssert.notNull(message, "Message");
        ArgAssert.notNull(callback, "Callback");
        ArgAssert.notNull(affinityKey, "Affinity key");

        gateway.aggregate(affinityKey, message, cluster, callback);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> MessagingChannel<C> withAffinityKey(Object affinityKey) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, failover, affinityKey, timeout);
    }

    @Override
    public MessagingChannelId getId() {
        return gateway.getId();
    }

    @Override
    public String getName() {
        return gateway.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> DefaultMessagingChannel<C> withLoadBalancer(LoadBalancer<C> balancer) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, failover, affinity, timeout);
    }

    @Override
    public Executor getExecutor() {
        return gateway.getExecutor();
    }

    @Override
    public Executor getExecutor(Object affinityKey) {
        return gateway.getExecutor(affinityKey);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> DefaultMessagingChannel<C> withFailover(FailoverPolicy policy) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, policy, affinity, timeout);
    }

    @Override
    public <C extends T> DefaultMessagingChannel<C> withFailover(FailoverPolicyBuilder policy) {
        return withFailover(policy.build());
    }

    @Override
    public DefaultMessagingChannel<T> withTimeout(long timeout, TimeUnit unit) {
        ArgAssert.notNull(unit, "Time unit");

        return new DefaultMessagingChannel<>(gateway, cluster, balancer, failover, affinity, unit.toMillis(timeout));
    }

    @Override
    public DefaultMessagingChannel<T> filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        return new DefaultMessagingChannel<>(gateway, cluster.filterAll(filter), balancer, failover, affinity, timeout);
    }

    @Override
    public ClusterView getCluster() {
        return cluster;
    }

    @Override
    public int getSockets() {
        return gateway.getSockets();
    }

    @Override
    public int getNioThreads() {
        return gateway.getNioThreads();
    }

    @Override
    public int getWorkerThreads() {
        return gateway.getWorkerThreads();
    }

    @Override
    public LoadBalancer<T> balancer() {
        return balancer;
    }

    @Override
    public ClusterView cluster() {
        return cluster;
    }

    @Override
    public FailoverPolicy failover() {
        return failover;
    }

    @Override
    public long timeout() {
        return timeout;
    }

    // Package level for testing purposes.
    MessagingGateway<T> getGateway() {
        return gateway;
    }

    @Override
    public String toString() {
        return ToString.format(MessagingChannel.class, this);
    }
}
