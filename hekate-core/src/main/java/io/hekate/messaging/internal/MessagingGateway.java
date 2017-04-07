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
import io.hekate.codec.CodecException;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.Waiting;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.failover.FailureResolution;
import io.hekate.failover.internal.DefaultFailoverContext;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
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
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

class MessagingGateway<T> {
    interface CloseCallback {
        void onBeforeClose();
    }

    private interface FailoverCallback {
        void retry(FailoverRoutingPolicy routingPolicy, FailureInfo newFailure);

        void fail(Throwable cause);
    }

    private static class PoolSelectionRejectedException extends Exception {
        private static final long serialVersionUID = 1L;

        public PoolSelectionRejectedException(Throwable cause) {
            super(null, cause, false, false);
        }
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
        MessageReceiver<T> receiver, int nioThreads, AffinityExecutor async, MetricsCallback metrics,
        ReceiveBackPressure receiveBackPressure, SendBackPressure sendBackPressure, FailoverPolicy failoverPolicy, long defaultTimeout,
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
        this.rootCluster = cluster;
        this.receiver = receiver;
        this.nioThreads = nioThreads;
        this.async = async;
        this.metrics = metrics;
        this.receiveBackPressure = receiveBackPressure;
        this.sendBackPressure = sendBackPressure;
        this.checkIdle = checkIdle;
        this.onBeforeClose = onBeforeClose;

        this.asyncAdaptor = new AffinityExecutorAdaptor(async);

        this.channel = new DefaultMessagingChannel<>(this, rootCluster, loadBalancer, failoverPolicy, null, defaultTimeout);
    }

    public MessagingChannelId getId() {
        return id;
    }

    public String getName() {
        return name;
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

        MessageContext<T> ctx = newContext(affinityKey, message, opts);

        try {
            long timeout = backPressureAcquire(opts.timeout(), message);

            routeAndSend(ctx, callback, null);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (RejectedExecutionException e) {
            notifyOnChannelClosedError(ctx, callback);
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

        MessageContext<T> ctx = newContext(affinityKey, message, opts);

        try {
            long timeout = backPressureAcquire(opts.timeout(), message);

            routeAndRequest(ctx, callback, null);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (RejectedExecutionException e) {
            notifyOnChannelClosedError(ctx, callback);
        }
    }

    public BroadcastFuture<T> broadcast(Object affinityKey, T message, MessagingOpts<T> opts) {
        BroadcastCallbackFuture<T> future = new BroadcastCallbackFuture<>();

        broadcast(affinityKey, message, opts, future);

        return future;
    }

    public void broadcast(Object affinityKey, T message, MessagingOpts<T> opts, BroadcastCallback<T> callback) {
        assert message != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        Set<ClusterNode> nodes = opts.cluster().getTopology().getNodes();

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyBroadcastResult<>(message));
        } else {
            BroadcastContext<T> broadcast = new BroadcastContext<>(message, nodes, callback);

            for (ClusterNode node : nodes) {
                send(affinityKey, message, opts.forSingleNode(node), err -> {
                    boolean completed;

                    if (err == null) {
                        completed = broadcast.onSendSuccess(node);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before we event tried.
                        completed = broadcast.forgetNode(node);
                    } else {
                        completed = broadcast.onSendFailure(node, err);
                    }

                    if (completed) {
                        broadcast.complete();
                    }
                });
            }
        }
    }

    @SuppressWarnings("unchecked") // <- need to cast to the response type.
    public <R extends T> AggregateFuture<R> aggregate(Object affinityKey, T message, MessagingOpts<T> opts) {
        AggregateCallbackFuture<T> future = new AggregateCallbackFuture<>();

        aggregate(affinityKey, message, opts, future);

        return (AggregateFuture<R>)future;
    }

    public void aggregate(Object affinityKey, T message, MessagingOpts<T> opts, AggregateCallback<T> callback) {
        assert message != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        Set<ClusterNode> nodes = opts.cluster().getTopology().getNodes();

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyAggregateResult<>(message));
        } else {
            AggregateContext<T> aggregate = new AggregateContext<>(message, nodes, callback);

            for (ClusterNode node : nodes) {
                request(affinityKey, message, opts.forSingleNode(node), (err, reply) -> {
                    boolean completed;

                    if (err == null) {
                        completed = aggregate.onReplySuccess(node, reply);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before we event tried.
                        completed = aggregate.forgetNode(node);
                    } else {
                        completed = aggregate.onReplyFailure(node, err);
                    }

                    if (completed) {
                        aggregate.complete();
                    }
                });
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

    private void routeAndSend(MessageContext<T> ctx, SendCallback callback, FailureInfo prevErr) {
        ClientPool<T> pool = null;

        try {
            pool = selectPool(ctx, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (PoolSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (pool != null) {
            doSend(ctx, pool, callback, prevErr);
        }
    }

    private void doSend(MessageContext<T> ctx, ClientPool<T> pool, SendCallback callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        SendCallback retryCallback = err -> {
            if (err == null || ctx.getOpts().failover() == null) {
                // Complete operation.
                if (ctx.complete()) {
                    backPressureRelease();

                    if (callback != null) {
                        callback.onComplete(err);
                    }
                }
            } else {
                // Apply failover actions.
                FailoverCallback onFailover = new FailoverCallback() {
                    @Override
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doSend(ctx, pool, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(pool.getNode())) {
                                    doSend(ctx, pool, callback, failure);
                                } else {
                                    routeAndSend(ctx, callback, failure);
                                }

                                break;
                            }
                            case RE_ROUTE: {
                                routeAndSend(ctx, callback, failure);

                                break;
                            }
                            default: {
                                throw new IllegalArgumentException("Unexpected routing policy: " + routing);
                            }
                        }
                    }

                    @Override
                    public void fail(Throwable cause) {
                        notifyOnErrorAsync(ctx, callback, cause);
                    }
                };

                applyFailoverAsync(ctx, err, pool.getNode(), onFailover, prevErr);
            }
        };

        pool.send(ctx, retryCallback);
    }

    private void routeAndRequest(MessageContext<T> ctx, RequestCallback<T> callback, FailureInfo prevErr) {
        ClientPool<T> pool = null;

        try {
            pool = selectPool(ctx, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (PoolSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (pool != null) {
            doRequest(ctx, pool, callback, prevErr);
        }
    }

    private void doRequest(MessageContext<T> ctx, ClientPool<T> pool, RequestCallback<T> callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        InternalRequestCallback<T> internalCallback = (request, err, reply) -> {
            T replyMsg = null;

            // Check if reply is an application-level error message.
            if (err == null) {
                replyMsg = reply.get();

                err = tryConvertToError(replyMsg);

                if (err != null) {
                    replyMsg = null;
                }
            }

            // Check if request callback can accept operation result.
            ReplyDecision acceptance = callback.accept(err, replyMsg);

            if (acceptance == null) {
                acceptance = ReplyDecision.DEFAULT;
            }

            FailoverPolicy failover = ctx.getOpts().failover();

            if (acceptance == ReplyDecision.COMPLETE || acceptance == ReplyDecision.DEFAULT && err == null || failover == null) {
                // Check if this is the final reply or an error (ignore chunks).
                if (err != null || !reply.isPartial()) {
                    // Make sure that callback will be notified only once.
                    if (ctx.complete()) {
                        request.unregister();

                        backPressureRelease();

                        // Accept final reply.
                        callback.onComplete(err, reply);
                    }
                } else if (!ctx.isCompleted()) {
                    // Accept chunk.
                    callback.onComplete(null, reply);
                }
            } else {
                // No more interactions with this request.
                // If failover is successful then new request will be registered.
                request.unregister();

                // Apply failover actions.
                if (acceptance == ReplyDecision.REJECT) {
                    err = new RejectedReplyException("Reply was rejected by a request callback", replyMsg, err);
                }

                FailoverCallback onFailover = new FailoverCallback() {
                    @Override
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doRequest(ctx, pool, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(pool.getNode())) {
                                    doRequest(ctx, pool, callback, failure);
                                } else {
                                    routeAndRequest(ctx, callback, failure);
                                }

                                break;
                            }
                            case RE_ROUTE: {
                                routeAndRequest(ctx, callback, failure);

                                break;
                            }
                            default: {
                                throw new IllegalArgumentException("Unexpected routing policy: " + routing);
                            }
                        }
                    }

                    @Override
                    public void fail(Throwable cause) {
                        notifyOnErrorAsync(ctx, callback, cause);
                    }
                };

                applyFailoverAsync(ctx, err, pool.getNode(), onFailover, prevErr);
            }
        };

        pool.request(ctx, internalCallback);
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
                        removed = emptySet();
                    }

                    if (added == null) {
                        added = emptySet();
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

    void scheduleTimeout(MessageContext<T> ctx, Object callback) {
        doScheduleTimeout(ctx.getOpts().timeout(), ctx, callback);
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

    private void doScheduleTimeout(long timeout, MessageContext<T> ctx, Object callback) {
        if (!ctx.isCompleted()) {
            try {
                Future<?> future = ctx.getWorker().executeDeferred(timeout, () -> {
                    if (ctx.completeOnTimeout()) {
                        T msg = ctx.getMessage();

                        doNotifyOnError(callback, new MessageTimeoutException("Messaging operation timed out [message=" + msg + ']'));
                    }
                });

                ctx.setTimeoutFuture(future);
            } catch (RejectedExecutionException e) {
                // Ignore since this error means that channel is closed.
                // In such case we can ignore timeout notification because messaging context will be notified by another error.
            }
        }
    }

    private boolean isKnownNode(ClusterNode node) {
        long readLock = lock.readLock();

        try {
            return pools.containsKey(node.getId());
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private ClientPool<T> selectPool(MessageContext<T> ctx, FailureInfo prevErr) throws HekateException, PoolSelectionRejectedException {
        assert ctx != null : "Message context is null.";

        ClusterTopology topology = ctx.getOpts().cluster().getTopology();

        if (topology.isEmpty()) {
            Throwable cause = prevErr != null ? prevErr.getError() : null;

            if (cause == null) {
                throw new UnknownRouteException("No suitable receivers [channel=" + name + ']');
            } else {
                throw new PoolSelectionRejectedException(cause);
            }
        }

        LoadBalancer<T> balancer = ctx.getOpts().balancer();

        while (true) {
            // Perform routing in unlocked context in order to prevent cluster events blocking.
            ClusterNodeId targetId;

            if (balancer == null) {
                // If balancer not specified then try to use cluster topology directly (must contain only one node).
                if (topology.size() > 1) {
                    Throwable cause = prevErr != null ? prevErr.getError() : null;

                    if (cause == null) {
                        throw new TooManyRoutesException("Too many receivers [channel=" + name + ", topology=" + topology + ']');
                    } else {
                        throw new PoolSelectionRejectedException(cause);
                    }
                }

                // Select the only one node from the cluster topology.
                targetId = topology.getNodes().iterator().next().getId();
            } else {
                LoadBalancerContext balancerCtx = new DefaultLoadBalancerContext(ctx, topology, Optional.ofNullable(prevErr));

                targetId = balancer.route(ctx.getMessage(), balancerCtx);

                if (targetId == null) {
                    Throwable cause = prevErr != null ? prevErr.getError() : null;

                    if (cause == null) {
                        throw new UnknownRouteException("Load balancer failed to select a target node.");
                    } else {
                        throw new PoolSelectionRejectedException(cause);
                    }
                }
            }

            // Enter lock (prevents channel state changes).
            long readLock = lock.readLock();

            try {
                // Check that channel is not closed.
                if (closed) {
                    throw channelClosedError(null);
                }

                ClientPool<T> pool = pools.get(targetId);

                if (pool == null) {
                    // Post-check that topology was not changed during routing.
                    ClusterTopology currentTopology = rootCluster.getTopology();

                    if (channelTopology != null && channelTopology.getVersion() == currentTopology.getVersion()) {
                        Throwable cause = prevErr != null ? prevErr.getError() : null;

                        if (cause == null) {
                            throw new UnknownRouteException("Node is not within the channel topology [id=" + targetId + ']');
                        } else {
                            throw new PoolSelectionRejectedException(cause);
                        }
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

    private void applyFailoverAsync(MessageContext<T> ctx, Throwable cause, ClusterNode failed, FailoverCallback callback,
        FailureInfo prevErr) {
        onAsyncEnqueue();

        ctx.getWorker().execute(() -> {
            onAsyncDequeue();

            applyFailover(ctx, cause, failed, callback, prevErr);
        });
    }

    private void applyFailover(MessageContext<T> ctx, Throwable cause, ClusterNode failed, FailoverCallback callback, FailureInfo prevErr) {
        assert ctx != null : "Message context is null.";
        assert cause != null : "Cause is null.";
        assert failed != null : "Failed node is null.";
        assert callback != null : "Failover callback is null";

        // Do nothing if operation is already completed.
        if (ctx.isCompleted()) {
            return;
        }

        boolean applied = false;

        Throwable finalCause = cause;

        FailoverPolicy policy = ctx.getOpts().failover();

        if (policy != null && isRecoverable(cause)) {
            int attempt;
            FailoverRoutingPolicy prevRouting;
            Set<ClusterNode> failedNodes;

            if (prevErr == null) {
                attempt = 0;
                prevRouting = RETRY_SAME_NODE;
                failedNodes = Collections.singleton(failed);
            } else {
                attempt = prevErr.getAttempt() + 1;
                prevRouting = prevErr.getRouting();

                failedNodes = new HashSet<>(prevErr.getFailedNodes());

                failedNodes.add(failed);

                failedNodes = unmodifiableSet(failedNodes);
            }

            DefaultFailoverContext failoverCtx = new DefaultFailoverContext(attempt, cause, failed, failedNodes, prevRouting);

            // Apply failover policy.
            try {
                FailureResolution resolution = policy.apply(failoverCtx);

                // Enter lock (prevents channel state changes).
                long readLock = lock.readLock();

                try {
                    if (closed) {
                        finalCause = channelClosedError(cause);
                    } else if (resolution != null && resolution.isRetry()) {
                        FailoverRoutingPolicy route = resolution.getRoutingPolicy();

                        // Apply failover only if re-routing was requested or if the target node is still within the channel's topology.
                        if (route != RETRY_SAME_NODE || pools.containsKey(failed.getId())) {
                            onRetry();

                            AffinityWorker worker = ctx.getWorker();

                            // Schedule timeout task to apply failover actions after the failover delay.
                            onAsyncEnqueue();

                            worker.executeDeferred(resolution.getDelay(), () -> {
                                onAsyncDequeue();

                                try {
                                    callback.retry(route, failoverCtx.withRouting(route));
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
            callback.fail(finalCause);
        }
    }

    private boolean isRecoverable(Throwable cause) {
        return !(cause instanceof MessagingChannelClosedException)
            && !(cause instanceof CodecException);
    }

    private ClientPool<T> createClientPool(ClusterNode node) {
        if (node.equals(localNode)) {
            return new InMemoryClientPool<>(localNode, this);
        } else {
            return new NetClientPool<>(name, node, net, this, checkIdle);
        }
    }

    private long backPressureAcquire(long timeout, T msg) throws MessageQueueOverflowException, InterruptedException,
        MessageQueueTimeoutException {
        if (sendBackPressure != null) {
            return sendBackPressure.onEnqueue(timeout, msg);
        }

        return timeout;
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
        MessagingException err = channelClosedError(null);

        notifyOnErrorAsync(ctx, callback, err);
    }

    private void notifyOnErrorAsync(MessageContext<T> ctx, Object callback, Throwable err) {
        onAsyncEnqueue();

        ctx.getWorker().execute(() -> {
            onAsyncDequeue();

            if (ctx.complete()) {
                doNotifyOnError(callback, err);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void doNotifyOnError(Object callback, Throwable err) {
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
            } else {
                throw new IllegalArgumentException("Unexpected callback type: " + callback);
            }
        }
    }

    private MessageContext<T> newContext(Object affinityKey, T message, MessagingOpts<T> opts) {
        int affinity = affinity(affinityKey);

        AffinityWorker worker = async.workerFor(affinity);

        return new MessageContext<>(message, affinity, affinityKey, worker, opts);
    }

    private MessagingChannelClosedException channelClosedError(Throwable cause) {
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
