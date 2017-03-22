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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.Waiting;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.failover.FailureResolution;
import io.hekate.failover.internal.DefaultFailoverContext;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.UnknownRouteException;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.broadcast.BroadcastResult;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.messaging.unicast.LoadBalancerContext;
import io.hekate.messaging.unicast.RejectedReplyException;
import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.ReplyFailure;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.TooManyRoutesException;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static java.util.stream.Collectors.toList;

class MessagingGateway<T> {
    interface CloseCallback {
        void onBeforeClose();
    }

    private interface FailoverAcceptCallback {
        void onAccept(FailoverRoutingPolicy routingPolicy, FailureInfo newFailure);
    }

    private interface FailoverRejectCallback {
        void onReject(Throwable cause);
    }

    private static class SendCallbackFuture extends SendFuture implements SendCallback {
        @Override
        public void onComplete(Throwable err) {
            if (err == null) {
                complete(null);
            } else {
                completeExceptionally(err);
            }
        }
    }

    private static class RequestCallbackFuture<T> extends RequestFuture<T> implements RequestCallback<T> {
        @Override
        public void onComplete(Throwable err, Reply<T> reply) {
            if (err == null) {
                if (!reply.isPartial()) {
                    complete(reply);
                }
            } else {
                completeExceptionally(err);
            }
        }
    }

    private static class BroadcastCallbackFuture<T> extends BroadcastFuture<T> implements BroadcastCallback<T> {
        @Override
        public void onComplete(Throwable err, BroadcastResult<T> result) {
            if (err == null) {
                complete(result);
            } else {
                completeExceptionally(err);
            }
        }
    }

    private static class AggregateCallbackFuture<T> extends AggregateFuture<T> implements AggregateCallback<T> {
        @Override
        public void onComplete(Throwable err, AggregateResult<T> result) {
            if (err == null) {
                complete(result);
            } else {
                completeExceptionally(err);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MessagingGateway.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    @ToStringIgnore
    private final MessagingChannelId id;

    @ToStringIgnore
    private final ClusterNode localNode;

    @ToStringIgnore
    private final NetworkConnector<MessagingProtocol> net;

    @ToStringIgnore
    private final ClusterView rootCluster;

    @ToStringIgnore
    private final MessageReceiver<T> receiver;

    @ToStringIgnore
    private final CloseCallback onBeforeClose;

    @ToStringIgnore
    private final Map<ClusterNodeId, ClientPool<T>> pools = new HashMap<>();

    @ToStringIgnore
    private final boolean checkIdle;

    @ToStringIgnore
    private final Set<NetReceiverContext<T>> serverReceivers = new HashSet<>();

    @ToStringIgnore
    private final StampedLock lock = new StampedLock();

    @ToStringIgnore
    private final AffinityExecutor async;

    @ToStringIgnore
    private final MetricsCallback metrics;

    @ToStringIgnore
    private final ReceiveBackPressure receiveBackPressure;

    @ToStringIgnore
    private final SendBackPressure sendBackPressure;

    @ToStringIgnore
    private final int sockets;

    @ToStringIgnore
    private final int nioThreads;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> channel;

    @ToStringIgnore
    private final AffinityExecutorAdaptor asyncAdaptor;

    @ToStringIgnore
    private ClusterTopology channelTopology;

    @ToStringIgnore
    private boolean closed;

    public MessagingGateway(String name, NetworkConnector<MessagingProtocol> net, ClusterNode localNode, ClusterView cluster,
        MessageReceiver<T> receiver, int sockets, int nioThreads, AffinityExecutor async, MetricsCallback metrics,
        ReceiveBackPressure receiveBackPressure, SendBackPressure sendBackPressure, FailoverPolicy failoverPolicy,
        LoadBalancer<T> loadBalancer, boolean checkIdle, CloseCallback onBeforeClose) {
        assert name != null : "Name is null.";
        assert net != null : "Network connector is null.";
        assert localNode != null : "Local cluster node is null.";
        assert cluster != null : "Cluster view is null.";
        assert async != null : "Executor is null.";

        this.id = new MessagingChannelId();
        this.name = name;
        this.net = net;
        this.localNode = localNode;
        this.receiver = receiver;
        this.sockets = sockets;
        this.nioThreads = nioThreads;
        this.async = async;
        this.metrics = metrics;
        this.receiveBackPressure = receiveBackPressure;
        this.sendBackPressure = sendBackPressure;
        this.checkIdle = checkIdle;
        this.onBeforeClose = onBeforeClose;

        this.rootCluster = cluster;

        this.asyncAdaptor = new AffinityExecutorAdaptor(async);

        this.channel = new DefaultMessagingChannel<>(this, rootCluster, loadBalancer, failoverPolicy, null);
    }

    public MessagingChannelId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public ClusterView getCluster() {
        return rootCluster;
    }

    public int getSockets() {
        return sockets;
    }

    public int getNioThreads() {
        return nioThreads;
    }

    public int getWorkerThreads() {
        return async.getThreadPoolSize();
    }

    public SendFuture send(Object affinityKey, T message, MessagingOpts<T> opts) {
        SendCallbackFuture future = new SendCallbackFuture();

        send(affinityKey, message, opts, future);

        return future;
    }

    public void send(Object affinityKey, T message, MessagingOpts<T> opts, SendCallback callback) {
        assert message != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, message);

        if (backPressureAcquire(ctx, callback)) {
            try {
                routeAndSend(ctx, opts, callback, null);
            } catch (RejectedExecutionException e) {
                notifyOnChannelClosedError(ctx, callback);
            }
        }
    }

    @SuppressWarnings("unchecked") // <- need to cast to the response type.
    public <R extends T> RequestFuture<R> request(Object affinityKey, T message, MessagingOpts<T> opts) {
        RequestCallbackFuture<T> future = new RequestCallbackFuture<>();

        request(affinityKey, message, opts, future);

        return (RequestFuture<R>)future;
    }

    public void request(Object affinityKey, T message, MessagingOpts<T> opts, RequestCallback<T> callback) {
        assert message != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, message);

        if (backPressureAcquire(ctx, callback)) {
            try {
                routeAndRequest(ctx, opts, callback, null);
            } catch (RejectedExecutionException e) {
                notifyOnChannelClosedError(ctx, callback);
            }
        }
    }

    public BroadcastFuture<T> broadcast(Object affinityKey, T message, ClusterView cluster) {
        BroadcastCallbackFuture<T> future = new BroadcastCallbackFuture<>();

        broadcast(affinityKey, message, cluster, future);

        return future;
    }

    public void broadcast(Object affinityKey, T message, ClusterView cluster, BroadcastCallback<T> callback) {
        assert message != null : "Message must be not null.";
        assert cluster != null : "Cluster must be not null.";
        assert callback != null : "Callback must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, message);

        if (backPressureAcquire(ctx, callback)) {
            try {
                doBroadcast(ctx, cluster, callback);
            } catch (RejectedExecutionException e) {
                notifyOnChannelClosedError(ctx, callback);
            }
        }
    }

    @SuppressWarnings("unchecked") // <- need to cast to the response type.
    public <R extends T> AggregateFuture<R> aggregate(Object affinityKey, T message, ClusterView cluster) {
        AggregateCallbackFuture<T> future = new AggregateCallbackFuture<>();

        aggregate(affinityKey, message, cluster, future);

        return (AggregateFuture<R>)future;
    }

    public void aggregate(Object affinityKey, T message, ClusterView cluster, AggregateCallback<T> callback) {
        assert message != null : "Message must be not null.";
        assert cluster != null : "Cluster must be not null.";
        assert callback != null : "Callback must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, message);

        if (backPressureAcquire(ctx, callback)) {
            try {
                doAggregate(ctx, cluster, callback);
            } catch (RejectedExecutionException e) {
                notifyOnChannelClosedError(ctx, callback);
            }
        }
    }

    public Waiting close() {
        // Notify on close callback.
        if (onBeforeClose != null) {
            onBeforeClose.onBeforeClose();
        }

        List<Waiting> waiting;

        long writeLock = lock.writeLock();

        try {
            if (closed) {
                return Waiting.NO_WAIT;
            } else {
                if (DEBUG) {
                    log.debug("Closing channel [name={}]", name);
                }

                // Mark as closed.
                closed = true;
                channelTopology = null;

                // Terminate back pressure guard.
                if (sendBackPressure != null) {
                    sendBackPressure.terminate();
                }

                // Close all client pools.
                List<NetworkFuture<MessagingProtocol>> disconnects = new LinkedList<>();

                for (ClientPool<T> pool : pools.values()) {
                    disconnects.addAll(pool.close());
                }

                // Clear client pools.
                pools.clear();

                // Close all server connections.
                List<NetReceiverContext<T>> receivers;

                synchronized (serverReceivers) {
                    // Create a local copy of receivers list since they are removing themselves from this list during disconnect.
                    receivers = new ArrayList<>(serverReceivers);

                    serverReceivers.clear();
                }

                disconnects.addAll(receivers.stream()
                    .map(NetReceiverContext::disconnect)
                    .filter(Objects::nonNull)
                    .collect(toList()));

                waiting = new LinkedList<>();

                // Collect disconnect futures to waiting list.
                disconnects.stream()
                    .map(future -> (Waiting)() -> {
                        try {
                            future.get();
                        } catch (ExecutionException e) {
                            Throwable cause = e.getCause();

                            if (cause instanceof IOException) {
                                if (DEBUG) {
                                    log.debug("Failed to close network connection due to an I/O error "
                                        + "[channel={}, cause={}]", name, cause.toString());
                                }
                            } else {
                                log.warn("Failed to close network connection [channel={}]", name, cause);
                            }
                        }
                    })
                    .forEach(waiting::add);

                // Terminate async thread pool.
                waiting.add(async::terminate);

                // Register waiting for async thread pool termination.
                waiting.add(async::awaitTermination);
            }
        } finally {
            lock.unlockWrite(writeLock);
        }

        return Waiting.awaitAll(waiting);
    }

    public MessageReceiver<T> getReceiver() {
        return receiver;
    }

    public Executor getExecutor() {
        return asyncAdaptor;
    }

    public Executor getExecutor(Object affinityKey) {
        return asyncAdaptor.getExecutor(affinityKey);
    }

    DefaultMessagingChannel<T> getChannel() {
        return channel;
    }

    AffinityExecutor getAsync() {
        return async;
    }

    MetricsCallback getMetrics() {
        return metrics;
    }

    ReceiveBackPressure getReceiveBackPressure() {
        return receiveBackPressure;
    }

    SendBackPressure getSendBackPressure() {
        return sendBackPressure;
    }

    private void routeAndSend(MessageContext<T> ctx, MessagingOpts<T> opts, SendCallback callback, FailureInfo prevErr) {
        ClientPool<T> pool = null;

        try {
            pool = selectUnicastPool(ctx, opts, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        }

        if (pool != null) {
            doSend(ctx, pool, opts, callback, prevErr);
        }
    }

    private void doSend(MessageContext<T> ctx, ClientPool<T> pool, MessagingOpts<T> opts, SendCallback callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        SendCallback decoratedCallback = err -> {
            if (err == null || opts.failover() == null) {
                if (ctx.complete()) {
                    backPressureRelease();

                    if (callback != null) {
                        callback.onComplete(err);
                    }
                }
            } else {
                // Actions if operation should be retried.
                FailoverAcceptCallback onFailover = (routingPolicy, failure) -> {
                    switch (routingPolicy) {
                        case RETRY_SAME_NODE: {
                            doSend(ctx, pool, opts, callback, failure);

                            break;
                        }
                        case PREFER_SAME_NODE: {
                            if (isKnownNode(pool.getNode())) {
                                doSend(ctx, pool, opts, callback, failure);
                            } else {
                                routeAndSend(ctx, opts, callback, failure);
                            }

                            break;
                        }
                        case RE_ROUTE: {
                            routeAndSend(ctx, opts, callback, failure);

                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected routing policy: " + routingPolicy);
                        }
                    }
                };

                // Actions if operation can't be retried.
                FailoverRejectCallback onFail = newCause -> notifyOnErrorAsync(ctx, callback, newCause);

                // Apply failover actions.
                applyFailoverAsync(ctx, err, pool.getNodeOpt(), opts, prevErr, onFailover, onFail);
            }
        };

        pool.send(ctx, decoratedCallback);
    }

    private void routeAndRequest(MessageContext<T> ctx, MessagingOpts<T> opts, RequestCallback<T> callback,
        FailureInfo prevErr) {
        ClientPool<T> pool = null;

        try {
            pool = selectUnicastPool(ctx, opts, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        }

        if (pool != null) {
            doRequest(ctx, pool, opts, callback, prevErr);
        }
    }

    private void doRequest(MessageContext<T> ctx, ClientPool<T> pool, MessagingOpts<T> opts, RequestCallback<T> callback,
        FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        RequestCallback<T> decoratedCallback = new RequestCallback<T>() {
            @Override
            public void onComplete(Throwable err, Reply<T> reply) {
                T replyMsg = null;

                // Check if reply is an application-level error message.
                if (err == null) {
                    replyMsg = reply.get();

                    err = tryConvertToError(replyMsg);

                    if (err != null) {
                        replyMsg = null;
                    }
                }

                ReplyDecision acceptance = ReplyDecision.DEFAULT;

                // Check if request callback can accept operation result.
                if (err != null || !reply.mustReply()) {
                    acceptance = callback.accept(err, replyMsg);
                }

                if (acceptance == null) {
                    acceptance = ReplyDecision.DEFAULT;
                }

                FailoverPolicy failover = opts.failover();

                if (acceptance == ReplyDecision.COMPLETE || acceptance == ReplyDecision.DEFAULT && err == null || failover == null) {
                    // Check if this is the final reply or an error (ignore chunks).
                    if (err != null || !reply.isPartial()) {
                        // Make sure that callback will be notified only once on final reply.
                        if (ctx.complete()) {
                            backPressureRelease();

                            // Accept final reply.
                            callback.onComplete(err, reply);
                        }
                    } else if (!ctx.isCompleted()) {
                        // Accept chunk.
                        callback.onComplete(null, reply);
                    }
                } else {
                    // Apply failover.
                    if (acceptance == ReplyDecision.REJECT) {
                        err = new RejectedReplyException("Reply was rejected by a request callback", replyMsg, err);
                    }

                    // Actions if operation should be retried.
                    FailoverAcceptCallback onFailover = (routingPolicy, failure) -> {
                        switch (routingPolicy) {
                            case RETRY_SAME_NODE: {
                                doRequest(ctx, pool, opts, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(pool.getNode())) {
                                    doRequest(ctx, pool, opts, callback, failure);
                                } else {
                                    routeAndRequest(ctx, opts, callback, failure);
                                }

                                break;
                            }
                            case RE_ROUTE: {
                                routeAndRequest(ctx, opts, callback, failure);

                                break;
                            }
                            default: {
                                throw new IllegalArgumentException("Unexpected routing policy: " + routingPolicy);
                            }
                        }
                    };

                    // Actions if operation can't be retried.
                    FailoverRejectCallback onFail = newCause -> notifyOnErrorAsync(ctx, callback, newCause);

                    // Apply failover actions.
                    applyFailoverAsync(ctx, err, pool.getNodeOpt(), opts, prevErr, onFailover, onFail);
                }
            }

            @Override
            public ReplyDecision accept(Throwable err, T reply) {
                throw new AssertionError("Invalid API usage.");
            }
        };

        pool.request(ctx, decoratedCallback);
    }

    private boolean isKnownNode(ClusterNode node) {
        long readLock = lock.readLock();

        try {
            return pools.containsKey(node.getId());
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private void doAggregate(MessageContext<T> ctx, ClusterView cluster, AggregateCallback<T> callback) {
        Map<ClusterNode, ClientPool<T>> pools;

        try {
            pools = selectBroadcastPools(cluster);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);

            return;
        }

        if (pools.isEmpty()) {
            callback.onComplete(null, new ImmediateAggregateResult<>(ctx.getMessage()));
        } else {
            AggregateContext<T> aggregateCtx = new AggregateContext<>(ctx.getMessage(), pools.keySet(), callback);

            for (Map.Entry<ClusterNode, ClientPool<T>> e : pools.entrySet()) {
                ClusterNode target = e.getKey();
                ClientPool<T> pool = e.getValue();

                RequestCallback<T> requestCallback = (err, reply) -> {
                    if (err == null) {
                        // Check if reply is an application-level error message.
                        err = tryConvertToError(reply.get());
                    }

                    boolean completed;

                    if (err == null) {
                        completed = aggregateCtx.onReplySuccess(target, reply);
                    } else {
                        completed = aggregateCtx.onReplyFailure(target, err);
                    }

                    if (completed && ctx.complete()) {
                        backPressureRelease();

                        aggregateCtx.complete();
                    }
                };

                if (pool != null) {
                    pool.request(ctx, requestCallback);
                } else {
                    UnknownRouteException cause = new UnknownRouteException("Node is not within the channel topology "
                        + "[node=" + target + ']');

                    try {
                        requestCallback.onComplete(cause, null);
                    } catch (RuntimeException | Error err) {
                        log.error("Got an unexpected runtime error while notifying on another error [cause={}]", cause, err);
                    }
                }
            }
        }
    }

    private void doBroadcast(MessageContext<T> ctx, ClusterView cluster, BroadcastCallback<T> callback) {
        Map<ClusterNode, ClientPool<T>> pools;

        try {
            pools = selectBroadcastPools(cluster);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);

            return;
        }

        if (pools.isEmpty()) {
            callback.onComplete(null, new ImmediateBroadcastResult<>(ctx.getMessage()));
        } else {
            BroadcastContext<T> broadcastCtx = new BroadcastContext<>(ctx.getMessage(), pools.keySet(), callback);

            for (Map.Entry<ClusterNode, ClientPool<T>> e : pools.entrySet()) {
                ClusterNode node = e.getKey();
                ClientPool<T> pool = e.getValue();

                SendCallback sendCallback = err -> {
                    boolean completed;

                    if (err == null) {
                        completed = broadcastCtx.onSendSuccess(node);
                    } else {
                        completed = broadcastCtx.onSendFailure(node, err);
                    }

                    if (completed && ctx.complete()) {
                        backPressureRelease();

                        broadcastCtx.complete();
                    }
                };

                if (pool != null) {
                    pool.send(ctx, sendCallback);
                } else {
                    UnknownRouteException cause = new UnknownRouteException("Node is not within the channel topology [node=" + node + ']');

                    try {
                        sendCallback.onComplete(cause);
                    } catch (Throwable t) {
                        log.error("Got an unexpected runtime error while notifying callback on another error [cause={}]", cause, t);
                    }
                }
            }
        }
    }

    boolean registerServerReceiver(NetReceiverContext<T> receiver) {
        long readLock = lock.readLock();

        try {
            if (closed) {
                return false;
            }

            synchronized (serverReceivers) {
                serverReceivers.add(receiver);
            }

            return true;
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void unregisterServerReceiver(NetReceiverContext<T> receiver) {
        long readLock = lock.readLock();

        try {
            synchronized (serverReceivers) {
                serverReceivers.remove(receiver);
            }
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void checkIdleConnections() {
        long readLock = lock.readLock();

        try {
            if (!closed) {
                pools.values().forEach(ClientPool::disconnectIfIdle);
            }
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void updateTopology() {
        List<ClientPool<T>> poolsToClose = null;

        long writeLock = lock.writeLock();

        try {
            if (!closed) {
                ClusterTopology newTopology = rootCluster.getTopology();

                if (channelTopology == null || channelTopology.getVersion() < newTopology.getVersion()) {
                    if (DEBUG) {
                        log.debug("Updating topology [channel={}, topology={}]", name, newTopology);
                    }

                    Set<ClusterNode> newNodes = newTopology.getNodes();

                    Set<ClusterNode> added = null;
                    Set<ClusterNode> removed = null;

                    if (channelTopology == null) {
                        added = new HashSet<>(newNodes);
                    } else {
                        for (ClusterNode node : newNodes) {
                            if (!channelTopology.contains(node)) {
                                if (added == null) {
                                    added = new HashSet<>(newNodes.size(), 1.0f);
                                }

                                added.add(node);
                            }
                        }

                        for (ClusterNode node : channelTopology) {
                            if (!newNodes.contains(node)) {
                                if (removed == null) {
                                    removed = new HashSet<>(newNodes.size(), 1.0f);
                                }

                                removed.add(node);
                            }
                        }
                    }

                    if (removed == null) {
                        removed = Collections.emptySet();
                    }

                    if (added == null) {
                        added = Collections.emptySet();
                    }

                    if (!removed.isEmpty()) {
                        poolsToClose = removed.stream()
                            .map(node -> pools.remove(node.getId()))
                            .filter(Objects::nonNull)
                            .collect(toList());
                    }

                    if (!added.isEmpty()) {
                        added.forEach(node -> {
                            ClientPool<T> pool = createClientPool(node);

                            pools.put(node.getId(), pool);
                        });
                    }

                    this.channelTopology = newTopology;
                }
            }
        } finally {
            lock.unlockWrite(writeLock);
        }

        if (poolsToClose != null) {
            poolsToClose.forEach(ClientPool::close);
        }
    }

    // This method is for testing purposes only.
    ClientPool<T> getPool(ClusterNodeId nodeId) throws MessagingException {
        long readLock = lock.readLock();

        try {
            return pools.get(nodeId);
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private ClientPool<T> selectUnicastPool(MessageContext<T> ctx, MessagingOpts<T> opts, FailureInfo prevErr)
        throws HekateException {
        ClusterView cluster = opts.cluster();
        LoadBalancer<T> balancer = opts.balancer();

        while (true) {
            // Perform routing in unlocked context in order to prevent cluster events blocking.
            ClusterNodeId targetId;

            ClusterTopology topology = cluster.getTopology();

            if (balancer == null) {
                // If balancer not specified then try to use cluster topology directly (must contain only one node).
                if (topology.isEmpty()) {
                    throw new UnknownRouteException("No suitable receivers in the cluster topology [channel=" + name + ']');
                } else if (topology.size() > 1) {
                    throw new TooManyRoutesException("Too many receivers for unicast operation [channel=" + name
                        + ", topology=" + topology + ']');
                } else {
                    targetId = topology.getNodesList().get(0).getId();
                }
            } else {
                LoadBalancerContext loadBalance = loadBalancer(ctx, topology, prevErr);

                if (loadBalance.isEmpty()) {
                    throw new UnknownRouteException("No suitable receivers in the cluster topology [channel=" + name + ']');
                } else {
                    targetId = balancer.route(ctx.getMessage(), loadBalance);

                    if (targetId == null) {
                        throw new UnknownRouteException("Load balancer failed to select a target node.");
                    }
                }
            }

            // Enter lock (to prevent concurrent topology changes).
            long readLock = lock.readLock();

            try {
                // Check that channel was not closed.
                if (closed) {
                    throw getChannelClosedError(null);
                }

                ClientPool<T> pool = pools.get(targetId);

                if (pool == null) {
                    // Post-check that topology was not changed during routing.
                    ClusterTopology currentTopology = rootCluster.getTopology();

                    if (channelTopology != null && channelTopology.getVersion() == currentTopology.getVersion()) {
                        throw new UnknownRouteException("Node is not within the channel topology [id=" + targetId + ']');
                    }
                } else {
                    return pool;
                }
            } finally {
                lock.unlockRead(readLock);
            }

            // Since we are here it means that topology was changed during routing.
            if (DEBUG) {
                log.debug("Retrying routing since topology was changed [balancer={}]", balancer);
            }

            updateTopology();
        }
    }

    private Map<ClusterNode, ClientPool<T>> selectBroadcastPools(ClusterView cluster) throws HekateException {
        while (true) {
            ClusterTopology topology = cluster.getTopology();

            if (topology.isEmpty()) {
                return Collections.emptyMap();
            }

            // Enter lock (to prevent concurrent topology changes).
            long readLock = lock.readLock();

            try {
                // Check that channel was not closed.
                if (closed) {
                    throw getChannelClosedError(null);
                }

                if (topology.isEmpty()) {
                    return Collections.emptyMap();
                } else {
                    Map<ClusterNode, ClientPool<T>> targetPools = new HashMap<>(topology.size(), 1.0f);

                    boolean verifyTopology = false;

                    for (ClusterNode target : topology) {
                        if (target != null) {
                            ClientPool<T> pool = pools.get(target.getId());

                            if (pool == null) {
                                // Enable post-checking of topology changes.
                                verifyTopology = true;
                            }

                            targetPools.put(target, pool);
                        }
                    }

                    if (verifyTopology) {
                        // Post-check that topology was not changed during routing.
                        ClusterTopology currentTopology = rootCluster.getTopology();

                        if (topology.getVersion() == currentTopology.getVersion()) {
                            return targetPools;
                        }
                    } else {
                        return targetPools;
                    }
                }
            } finally {
                lock.unlockRead(readLock);
            }

            // Since we are here it means that topology was changed during routing.
            if (DEBUG) {
                log.debug("Retrying broadcast routing since topology was changed.");
            }

            updateTopology();
        }
    }

    private void applyFailoverAsync(MessageContext<T> ctx, Throwable cause, Optional<ClusterNode> failedNode,
        MessagingOpts<T> opts, FailureInfo prevErr, FailoverAcceptCallback onAccept, FailoverRejectCallback onReject) {
        boolean applied = false;

        if (async.isAsync()) {
            long readLock = lock.readLock();

            try {
                if (!closed) {
                    applied = true;

                    onAsyncEnqueue();

                    ctx.getWorker().execute(() -> {
                        onAsyncDequeue();

                        applyFailover(ctx, cause, failedNode, opts, prevErr, onAccept, onReject);
                    });
                }
            } finally {
                lock.unlock(readLock);
            }
        }

        if (!applied) {
            applyFailover(ctx, cause, failedNode, opts, prevErr, onAccept, onReject);
        }
    }

    private void applyFailover(MessageContext<T> ctx, Throwable cause, Optional<ClusterNode> failedNode, MessagingOpts<T> opts,
        FailureInfo prevErr, FailoverAcceptCallback onAccept, FailoverRejectCallback onReject) {
        FailoverPolicy policy = opts.failover();

        onRetry();

        boolean applied = false;

        Throwable finalCause = cause;

        if (policy != null && !(cause instanceof MessagingChannelClosedException)) {
            int attempt;
            FailoverRoutingPolicy prevRouting;
            Set<ClusterNode> failedNodes;

            if (prevErr == null) {
                attempt = 0;
                prevRouting = RETRY_SAME_NODE;
                failedNodes = failedNode.isPresent() ? Collections.singleton(failedNode.get()) : Collections.emptySet();
            } else {
                attempt = prevErr.getAttempt() + 1;
                prevRouting = prevErr.getRouting();

                failedNodes = new HashSet<>(prevErr.getFailedNodes());

                if (failedNode.isPresent()) {
                    failedNodes.add(failedNode.get());
                }

                failedNodes = Collections.unmodifiableSet(failedNodes);
            }

            DefaultFailoverContext newCtx = new DefaultFailoverContext(attempt, cause, failedNode, failedNodes, prevRouting);

            // Apply failover policy.
            try {
                FailureResolution resolution = policy.apply(newCtx);

                // Enter lock (prevents concurrent topology changes).
                long readLock = lock.readLock();

                try {
                    if (closed) {
                        finalCause = getChannelClosedError(cause);
                    } else if (resolution != null && resolution.isRetry()) {
                        long delay = resolution.getDelay();

                        FailoverRoutingPolicy reRoutePolicy = resolution.getRoutingPolicy();

                        // Apply failover only if re-routing was request or if the target node is still within the channel's topology.
                        if (reRoutePolicy != RETRY_SAME_NODE || failedNode.isPresent() && pools.containsKey(failedNode.get().getId())) {
                            // Schedule failover task for asynchronous execution.
                            onAsyncEnqueue();

                            AffinityWorker worker = ctx.getWorker();

                            worker.executeDeferred(delay, () -> {
                                onAsyncDequeue();

                                try {
                                    if (worker.isShutdown()) {
                                        // Channel was closed.
                                        onReject.onReject(getChannelClosedError(cause));
                                    } else {
                                        onAccept.onAccept(reRoutePolicy, newCtx.withRouting(reRoutePolicy));
                                    }
                                } catch (RuntimeException | Error e) {
                                    log.error("Got an unexpected error during failover task processing.", e);
                                }
                            });

                            applied = true;
                        }
                    }
                } finally {
                    lock.unlockRead(readLock);
                }
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected error while applying failover policy.", e);
            }
        }

        if (!applied) {
            onReject.onReject(finalCause);
        }
    }

    private LoadBalancerContext loadBalancer(MessageContext<T> ctx, ClusterTopology topology, FailureInfo failure)
        throws MessagingException {
        return new DefaultLoadBalancerContext(ctx.getAffinity(), ctx.getAffinityKey(), topology, Optional.ofNullable(failure));
    }

    private ClientPool<T> createClientPool(ClusterNode node) {
        if (node.equals(localNode)) {
            return new InMemoryClientPool<>(localNode, this);
        } else {
            return new NetClientPool<>(name, node, net, sockets, this, checkIdle);
        }
    }

    private boolean backPressureAcquire(MessageContext<T> ctx, Object callback) {
        if (sendBackPressure != null) {
            Exception err = sendBackPressure.onEnqueue();

            if (err != null) {
                affinityRun(ctx.getAffinity(), () ->
                    notifyOnErrorAsync(ctx, callback, err)
                );

                return false;
            }
        }

        return true;
    }

    private void backPressureRelease() {
        if (sendBackPressure != null) {
            sendBackPressure.onDequeue();
        }
    }

    private void onAsyncDequeue() {
        if (metrics != null) {
            metrics.onAsyncEnqueue();
        }
    }

    private void onAsyncEnqueue() {
        if (metrics != null) {
            metrics.onAsyncDequeue();
        }
    }

    private void onRetry() {
        if (metrics != null) {
            metrics.onRetry();
        }
    }

    private void notifyOnChannelClosedError(MessageContext<T> ctx, Object callback) {
        MessagingException err = getChannelClosedError(null);

        notifyOnErrorAsync(ctx, callback, err);
    }

    private void notifyOnErrorAsync(MessageContext<T> ctx, Object callback, Throwable err) {
        boolean scheduled = false;

        if (!ctx.getWorker().isShutdown()) {
            try {
                ctx.getWorker().execute(() ->
                    doNotifyOnError(ctx, callback, err)
                );

                scheduled = true;
            } catch (RejectedExecutionException e) {
                // Ignore since operation will be re-scheduled on common pool.
            }
        }

        if (!scheduled) {
            ForkJoinPool.commonPool().submit(() ->
                doNotifyOnError(ctx, callback, err)
            );
        }
    }

    @SuppressWarnings("unchecked")
    private void doNotifyOnError(MessageContext<T> ctx, Object callback, Throwable err) {
        if (ctx.complete()) {
            backPressureRelease();

            if (callback != null) {
                if (callback instanceof SendCallback) {
                    try {
                        ((SendCallback)callback).onComplete(err);
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while notifying send callback on another error [cause={}]", err, e);
                    }
                } else if (callback instanceof RequestCallback) {
                    try {
                        ((RequestCallback)callback).onComplete(err, null);
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while notifying request callback on another error [cause={}]", err, e);
                    }
                } else if (callback instanceof BroadcastCallback) {
                    try {
                        ((BroadcastCallback)callback).onComplete(err, null);
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while notifying send callback on another error [cause={}]", err, e);
                    }
                } else if (callback instanceof AggregateCallback) {
                    try {
                        ((AggregateCallback<Object>)callback).onComplete(err, null);
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while notifying send callback on another error [cause={}]", err, e);
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected callback type: " + callback);
                }
            }
        }
    }

    private MessageContext<T> newContext(Object affinityKey, T message) {
        int affinity = affinity(affinityKey);

        AffinityWorker worker = async.workerFor(affinity);

        return new MessageContext<>(message, affinity, affinityKey, worker);
    }

    private void affinityRun(int affinity, Runnable task) {
        if (async.isAsync()) {
            AffinityWorker worker = async.workerFor(affinity);

            boolean applied = false;

            if (!worker.isShutdown()) {
                try {
                    worker.execute(task);

                    applied = true;
                } catch (RejectedExecutionException e) {
                    // No-op.
                }
            }

            if (!applied) {
                ForkJoinPool.commonPool().execute(task);
            }
        } else {
            task.run();
        }
    }

    private MessagingChannelClosedException getChannelClosedError(Throwable cause) {
        return new MessagingChannelClosedException("Channel closed [channel=" + name + ']', cause);
    }

    private Throwable tryConvertToError(T replyMsg) {
        Throwable err = null;

        // Check if message should be converted to an error.
        if (replyMsg instanceof ReplyFailure) {
            err = ((ReplyFailure)replyMsg).asError();

            if (err == null) {
                err = new IllegalArgumentException(ReplyFailure.class.getSimpleName() + " message returned null error "
                    + "[message=" + replyMsg + ']');
            }
        }

        return err;
    }

    private static int affinity(Object key) {
        if (key == null) {
            // Use random value that is < 0.
            return -(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) + 1);
        } else {
            // Use positive value that is >= 0.
            int hash = key.hashCode();

            return hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hash);
        }
    }

    @Override
    public String toString() {
        return ToString.format(MessagingChannel.class, this);
    }
}
