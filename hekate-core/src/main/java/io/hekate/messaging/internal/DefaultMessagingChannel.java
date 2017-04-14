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
import io.hekate.cluster.ClusterNode;
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
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

class DefaultMessagingChannel<T> implements MessagingChannel<T>, MessagingOpts<T> {
    private final MessagingGateway<T> gateway;

    @ToStringIgnore
    private final ClusterView cluster;

    private final FailoverPolicy failover;

    private final LoadBalancer<T> balancer;

    private final Object affinityKey;

    private final long timeout;

    public DefaultMessagingChannel(MessagingGateway<T> gateway, ClusterView cluster, LoadBalancer<T> balancer, FailoverPolicy failover,
        long timeout, Object affinityKey) {
        assert gateway != null : "Gateway is null.";
        assert cluster != null : "Cluster view is null.";

        this.gateway = gateway;
        this.cluster = cluster;
        this.balancer = balancer;
        this.failover = failover;
        this.timeout = timeout;
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
    public <R extends T> ResponseFuture<R> request(T request) {
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
        gateway.subscribe(affinityKey, request, this, callback);
    }

    @Override
    public SubscribeFuture<T> subscribe(T request) {
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
    public <R extends T> AggregateFuture<R> aggregate(T message) {
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
    public MessagingChannelId getId() {
        return gateway.getId();
    }

    @Override
    public String getName() {
        return gateway.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> MessagingChannel<C> withAffinity(Object affinityKey) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, failover, timeout, affinityKey);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> DefaultMessagingChannel<C> withLoadBalancer(LoadBalancer<C> balancer) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, failover, timeout, affinityKey);
    }

    @Override
    public Executor getExecutor() {
        return gateway.getExecutor();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends T> DefaultMessagingChannel<C> withFailover(FailoverPolicy policy) {
        return new DefaultMessagingChannel(gateway, cluster, balancer, policy, timeout, affinityKey);
    }

    @Override
    public <C extends T> DefaultMessagingChannel<C> withFailover(FailoverPolicyBuilder policy) {
        return withFailover(policy.build());
    }

    @Override
    public DefaultMessagingChannel<T> withTimeout(long timeout, TimeUnit unit) {
        ArgAssert.notNull(unit, "Time unit");

        return new DefaultMessagingChannel<>(gateway, cluster, balancer, failover, unit.toMillis(timeout), affinityKey);
    }

    @Override
    public DefaultMessagingChannel<T> filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        return new DefaultMessagingChannel<>(gateway, cluster.filterAll(filter), balancer, failover, timeout, affinityKey);
    }

    @Override
    public ClusterView getCluster() {
        return cluster;
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

    @Override
    public MessagingOpts<T> forSingleNode(ClusterNode node) {
        ArgAssert.notNull(node, "Node");

        return new DefaultMessagingChannel<>(gateway, cluster.forNode(node), null, failover, timeout, affinityKey);
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
