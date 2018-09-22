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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.CodecException;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.HekateSupport;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.failover.FailureResolution;
import io.hekate.failover.internal.DefaultFailoverContext;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.unicast.FailureResponse;
import io.hekate.messaging.unicast.RejectedReplyException;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;
import org.slf4j.Logger;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

class MessagingGatewayContext<T> implements HekateSupport {
    interface CloseCallback {
        void onBeforeClose();
    }

    private interface FailoverCallback {
        void retry(FailoverRoutingPolicy routingPolicy, Optional<FailureInfo> newFailure);

        void fail(Throwable cause);
    }

    @FunctionalInterface
    private interface Router<T> {
        ClusterNodeId route(
            MessageContext<T> ctx,
            Optional<FailureInfo> prevFailure,
            PartitionMapper mapper,
            ClusterTopology topology,
            HekateSupport hekate
        ) throws LoadBalancerException;
    }

    private static class ClientSelectionRejectedException extends Exception {
        private static final long serialVersionUID = 1;

        public ClientSelectionRejectedException(Throwable cause) {
            super(null, cause, false, false);
        }
    }

    private static final Router<?> UNICAST_ROUTER = (ctx, prevFailure, mapper, topology, hekate) -> {
        LoadBalancerContext balancerCtx = new DefaultLoadBalancerContext(ctx, topology, hekate, mapper, prevFailure);

        return ctx.opts().balancer().route(ctx.originalMessage(), balancerCtx);
    };

    private final String name;

    private final Class<T> baseType;

    @ToStringIgnore
    private final Logger log;

    @ToStringIgnore
    private final boolean debug;

    @ToStringIgnore
    private final MessagingChannelId id;

    @ToStringIgnore
    private final ClusterNode localNode;

    @ToStringIgnore
    private final HekateSupport hekate;

    @ToStringIgnore
    private final NetworkConnector<MessagingProtocol> net;

    @ToStringIgnore
    private final ClusterView cluster;

    @ToStringIgnore
    private final MessageReceiver<T> receiver;

    @ToStringIgnore
    private final CloseCallback onBeforeClose;

    @ToStringIgnore
    private final boolean checkIdle;

    @ToStringIgnore
    private final StampedLock lock = new StampedLock();

    @ToStringIgnore
    private final MessagingExecutor async;

    @ToStringIgnore
    private final MessagingMetrics metrics;

    @ToStringIgnore
    private final ReceivePressureGuard receivePressure;

    @ToStringIgnore
    private final SendPressureGuard sendPressure;

    @ToStringIgnore
    private final InterceptorManager<T> interceptor;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> channel;

    @ToStringIgnore
    private final Set<MessagingConnectionNetIn<T>> inbound = new HashSet<>();

    @ToStringIgnore
    private final Map<ClusterNodeId, MessagingClient<T>> clients = new HashMap<>();

    @ToStringIgnore
    private ClusterTopology clientsTopology;

    @ToStringIgnore
    private boolean closed;

    public MessagingGatewayContext(
        String name,
        HekateSupport hekate,
        Class<T> baseType,
        NetworkConnector<MessagingProtocol> net,
        ClusterNode localNode,
        MessageReceiver<T> receiver,
        MessagingExecutor async,
        MessagingMetrics metrics,
        ReceivePressureGuard receivePressure,
        SendPressureGuard sendPressure,
        InterceptorManager<T> interceptor,
        Logger log,
        boolean checkIdle,
        DefaultMessagingChannel<T> channel,
        CloseCallback onBeforeClose
    ) {
        assert name != null : "Name is null.";
        assert baseType != null : "Base type is null.";
        assert net != null : "Network connector is null.";
        assert localNode != null : "Local cluster node is null.";
        assert async != null : "Executor is null.";
        assert metrics != null : "Metrics are null.";
        assert channel != null : "Default channel is null.";

        this.id = new MessagingChannelId();
        this.name = name;
        this.baseType = baseType;
        this.hekate = hekate;
        this.net = net;
        this.localNode = localNode;
        this.cluster = channel.cluster();
        this.receiver = receiver;
        this.interceptor = interceptor;
        this.async = async;
        this.metrics = metrics;
        this.receivePressure = receivePressure;
        this.sendPressure = sendPressure;
        this.checkIdle = checkIdle;
        this.log = log;
        this.debug = log.isDebugEnabled();
        this.channel = channel;
        this.onBeforeClose = onBeforeClose;
    }

    public MessagingChannelId channelId() {
        return id;
    }

    public String name() {
        return name;
    }

    public ClusterNode localNode() {
        return localNode;
    }

    public InterceptorManager<T> intercept() {
        return interceptor;
    }

    public Logger log() {
        return log;
    }

    public SendFuture send(Object affinityKey, T msg, MessagingOpts<T> opts) {
        SendCallbackFuture future = new SendCallbackFuture();

        send(affinityKey, msg, opts, future);

        return future;
    }

    public void send(Object affinityKey, T msg, MessagingOpts<T> opts, SendCallback callback) {
        sendWithRouter(unicastRouter(), affinityKey, msg, opts, callback);
    }

    public ResponseFuture<T> request(Object affinityKey, T msg, MessagingOpts<T> opts) {
        ResponseCallbackFuture<T> future = new ResponseCallbackFuture<>();

        request(affinityKey, msg, opts, future);

        return future;
    }

    public void request(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        requestWithRouter(unicastRouter(), affinityKey, msg, opts, callback);
    }

    public SubscribeFuture<T> subscribe(Object affinityKey, T msg, MessagingOpts<T> opts) {
        SubscribeCallbackFuture<T> future = new SubscribeCallbackFuture<>();

        subscribe(affinityKey, msg, opts, future);

        return future;
    }

    public void subscribe(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        checkMessageType(msg);

        MessageContext<T> ctx = newContext(OutboundType.SUBSCRIBE, affinityKey, msg, opts);

        requestAsync(msg, ctx, unicastRouter(), callback);
    }

    public BroadcastFuture<T> broadcast(Object affinityKey, T msg, MessagingOpts<T> opts) {
        BroadcastCallbackFuture<T> future = new BroadcastCallbackFuture<>();

        broadcast(affinityKey, msg, opts, future);

        return future;
    }

    public void broadcast(Object affinityKey, T msg, MessagingOpts<T> opts, BroadcastCallback<T> callback) {
        checkMessageType(msg);

        List<ClusterNode> nodes = nodesForBroadcast(affinityKey, opts);

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyBroadcastResult<>(msg));
        } else {
            BroadcastContext<T> broadcast = new BroadcastContext<>(msg, nodes, callback);

            for (ClusterNode node : nodes) {
                // Always route to the same node.
                Router<T> router = (ctx, prevFailure, mapper, topology, hekate) -> node.id();

                sendWithRouter(router, affinityKey, msg, opts, err -> {
                    boolean completed;

                    if (err == null) {
                        completed = broadcast.onSendSuccess(node);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before we even tried.
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

    public AggregateFuture<T> aggregate(Object affinityKey, T msg, MessagingOpts<T> opts) {
        AggregateCallbackFuture<T> future = new AggregateCallbackFuture<>();

        aggregate(affinityKey, msg, opts, future);

        return future;
    }

    public void aggregate(Object affinityKey, T msg, MessagingOpts<T> opts, AggregateCallback<T> callback) {
        checkMessageType(msg);

        List<ClusterNode> nodes = nodesForBroadcast(affinityKey, opts);

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyAggregateResult<>(msg));
        } else {
            AggregateContext<T> aggregate = new AggregateContext<>(msg, nodes, callback);

            for (ClusterNode node : nodes) {
                // Always route to the same node.
                Router<T> router = (ctx, prevFailure, mapper, topology, hekate) -> node.id();

                requestWithRouter(router, affinityKey, msg, opts, (err, reply) -> {
                    boolean completed;

                    if (err == null) {
                        completed = aggregate.onReplySuccess(node, reply);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // It may happen that in some rare cases some node leaves the cluster at the same time with this operation.
                        // We exclude such a node from the results of the operation, as if there was no such node at all.
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
                if (debug) {
                    log.debug("Closing channel [name={}]", name);
                }

                // Mark as closed.
                closed = true;
                clientsTopology = null;

                // Terminate back pressure guard.
                if (sendPressure != null) {
                    sendPressure.terminate();
                }

                // Close all clients.
                List<NetworkFuture<MessagingProtocol>> disconnects = new ArrayList<>();

                for (MessagingClient<T> client : clients.values()) {
                    disconnects.addAll(client.close());
                }

                // Clear clients.
                clients.clear();

                // Close all inbound connections.
                List<MessagingConnectionNetIn<T>> localInbound;

                synchronized (inbound) {
                    // Create a local copy of inbound connections since they are removing themselves from the list during disconnect.
                    localInbound = new ArrayList<>(inbound);

                    inbound.clear();
                }

                localInbound.stream()
                    .map(MessagingConnectionNetIn::disconnect)
                    .filter(Objects::nonNull)
                    .forEach(disconnects::add);

                waiting = new ArrayList<>();

                // Collect disconnect futures to waiting list.
                disconnects.stream()
                    .map(future -> (Waiting)future::join)
                    .forEach(waiting::add);

                // Terminate async thread pool.
                waiting.add(async::terminate);
            }
        } finally {
            lock.unlockWrite(writeLock);
        }

        return Waiting.awaitAll(waiting);
    }

    public MessageReceiver<T> receiver() {
        return receiver;
    }

    public Executor executor() {
        return async.pooledWorker();
    }

    @Override
    public Hekate hekate() {
        return hekate.hekate();
    }

    public Class<T> baseType() {
        return baseType;
    }

    public ClusterView cluster() {
        return cluster;
    }

    DefaultMessagingChannel<T> channel() {
        return channel;
    }

    MessagingExecutor async() {
        return async;
    }

    MessagingMetrics metrics() {
        return metrics;
    }

    ReceivePressureGuard receiveGuard() {
        return receivePressure;
    }

    SendPressureGuard sendGuard() {
        return sendPressure;
    }

    private void sendWithRouter(Router<T> router, Object affinityKey, T msg, MessagingOpts<T> opts, SendCallback callback) {
        checkMessageType(msg);

        if (callback != null && opts.isConfirmReceive()) {
            MessageContext<T> ctx = newContext(OutboundType.SEND_WITH_ACK, affinityKey, msg, opts);

            requestAsync(msg, ctx, router, (err, rsp) -> callback.onComplete(err));
        } else {
            MessageContext<T> ctx = newContext(OutboundType.SEND_NO_ACK, affinityKey, msg, opts);

            try {
                long timeout = backPressureAcquire(ctx.opts().timeout(), msg);

                if (timeout > 0) {
                    doScheduleTimeout(timeout, ctx, callback);
                }

                routeAndSend(ctx, router, callback, Optional.empty());
            } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
                notifyOnErrorAsync(ctx, callback, e);
            }
        }
    }

    private void routeAndSend(MessageContext<T> ctx, Router<T> router, SendCallback callback, Optional<FailureInfo> prevFailure) {
        MessageAttempt<T> attempt = null;

        try {
            attempt = route(ctx, router, prevFailure);
        } catch (HekateException e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during message routing.", e);
            }

            notifyOnErrorAsync(ctx, callback, e);
        } catch (ClientSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (attempt != null) {
            doSend(attempt, router, callback);
        }
    }

    private void doSend(MessageAttempt<T> attempt, Router<T> router, SendCallback callback) {
        MessageContext<T> ctx = attempt.ctx();

        // Decorate callback with failover, error handling logic, etc.
        SendCallback retryCallback = err -> {
            attempt.client().touch();

            if (err == null || ctx.opts().failover() == null) {
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
                    public void retry(FailoverRoutingPolicy routing, Optional<FailureInfo> failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doSend(attempt.newAttempt(failure), router, callback);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(attempt.client().node())) {
                                    doSend(attempt.newAttempt(failure), router, callback);
                                } else {
                                    routeAndSend(ctx, router, callback, failure);
                                }

                                break;
                            }
                            case RE_ROUTE: {
                                routeAndSend(ctx, router, callback, failure);

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

                applyFailoverAsync(attempt, err, onFailover);
            }
        };

        attempt.client().send(attempt, retryCallback);
    }

    private void requestWithRouter(Router<T> router, Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        checkMessageType(msg);

        MessageContext<T> ctx = newContext(OutboundType.REQUEST, affinityKey, msg, opts);

        requestAsync(msg, ctx, router, callback);
    }

    private void requestAsync(T request, MessageContext<T> ctx, Router<T> router, ResponseCallback<T> callback) {
        try {
            long timeout = backPressureAcquire(ctx.opts().timeout(), request);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }

            routeAndRequest(ctx, router, callback, Optional.empty());
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        }
    }

    private void routeAndRequest(MessageContext<T> ctx, Router<T> router, ResponseCallback<T> callback, Optional<FailureInfo> prevFailure) {
        MessageAttempt<T> attempt = null;

        try {
            attempt = route(ctx, router, prevFailure);
        } catch (HekateException e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during message routing.", e);
            }

            notifyOnErrorAsync(ctx, callback, e);
        } catch (ClientSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (attempt != null) {
            doRequest(attempt, router, callback);
        }
    }

    private void doRequest(MessageAttempt<T> attempt, Router<T> router, ResponseCallback<T> callback) {
        // Decorate callback with failover, error handling logic, etc.
        final MessageContext<T> ctx = attempt.ctx();

        InternalRequestCallback<T> internalCallback = (request, err, reply) -> {
            attempt.client().touch();

            // Check if reply is an application-level error message.
            if (err == null) {
                err = tryConvertToError(reply, attempt.receiver());
            }

            // Resolve effective reply.
            Response<T> effectiveReply = err == null ? reply : null;

            // Check if request callback can accept the response.
            ReplyDecision decision = callback.accept(err, effectiveReply);

            if (decision == null) {
                decision = ReplyDecision.DEFAULT;
            }

            FailoverPolicy failover = ctx.opts().failover();

            if ((decision == ReplyDecision.COMPLETE) || (decision == ReplyDecision.DEFAULT && err == null) || (failover == null)) {
                // Check if this is the final response or an error (ignore chunks).
                if (err != null || isFinalOrVoidResponse(reply)) {
                    /////////////////////////////////////////////////////////////
                    // Final response or error.
                    /////////////////////////////////////////////////////////////
                    // Make sure that callback will be notified only once.
                    if (ctx.complete()) {
                        request.unregister();

                        backPressureRelease();

                        // Accept final reply.
                        callback.onComplete(err, reply);
                    }
                } else if (!ctx.isCompleted()) {
                    /////////////////////////////////////////////////////////////
                    // Response chunk.
                    /////////////////////////////////////////////////////////////
                    // Ensure that stream doesn't get timed out.
                    if (ctx.opts().hasTimeout()) {
                        ctx.keepAlive();
                    }

                    // Accept chunk.
                    callback.onComplete(null, reply);
                }
            } else if (!ctx.isCompleted()) {
                /////////////////////////////////////////////////////////////
                // Apply failover.
                /////////////////////////////////////////////////////////////
                // Unregister request (if failover actions will be successful then a new request will be registered).
                request.unregister();

                // Apply failover actions.
                if (decision == ReplyDecision.REJECT) {
                    Object rejected = reply != null ? reply.get() : null;

                    err = new RejectedReplyException("Response was rejected by the request callback", rejected, err);
                }

                // Failover callback.
                FailoverCallback onFailover = new FailoverCallback() {
                    @Override
                    public void retry(FailoverRoutingPolicy routing, Optional<FailureInfo> failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doRequest(attempt.newAttempt(failure), router, callback);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(attempt.client().node())) {
                                    doRequest(attempt.newAttempt(failure), router, callback);
                                } else {
                                    routeAndRequest(ctx, router, callback, failure);
                                }

                                break;
                            }
                            case RE_ROUTE: {
                                routeAndRequest(ctx, router, callback, failure);

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

                // Apply failover.
                applyFailoverAsync(attempt, err, onFailover);
            }
        };

        attempt.client().request(attempt, internalCallback);
    }

    boolean register(MessagingConnectionNetIn<T> conn) {
        long readLock = lock.readLock();

        try {
            if (closed) {
                return false;
            }

            synchronized (inbound) {
                inbound.add(conn);
            }

            return true;
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void unregister(MessagingConnectionNetIn<T> conn) {
        long readLock = lock.readLock();

        try {
            synchronized (inbound) {
                inbound.remove(conn);
            }
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void checkIdleConnections() {
        long readLock = lock.readLock();

        try {
            if (!closed) {
                clients.values().forEach(MessagingClient::disconnectIfIdle);
            }
        } finally {
            lock.unlockRead(readLock);
        }
    }

    void updateTopology() {
        List<MessagingClient<T>> clientsToClose = null;

        long writeLock = lock.writeLock();

        try {
            if (!closed) {
                ClusterTopology newTopology = cluster.topology();

                if (clientsTopology == null || clientsTopology.version() < newTopology.version()) {
                    if (debug) {
                        log.debug("Updating topology [channel={}, topology={}]", name, newTopology);
                    }

                    Set<ClusterNode> newNodes = newTopology.nodeSet();

                    Set<ClusterNode> added = null;
                    Set<ClusterNode> removed = null;

                    if (clientsTopology == null) {
                        added = new HashSet<>(newNodes);
                    } else {
                        for (ClusterNode node : newNodes) {
                            if (!clientsTopology.contains(node)) {
                                if (added == null) {
                                    added = new HashSet<>(newNodes.size(), 1.0f);
                                }

                                added.add(node);
                            }
                        }

                        for (ClusterNode node : clientsTopology) {
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
                        clientsToClose = removed.stream()
                            .map(node -> clients.remove(node.id()))
                            .filter(Objects::nonNull)
                            .collect(toList());
                    }

                    if (!added.isEmpty()) {
                        added.forEach(node -> {
                            MessagingClient<T> client = createClient(node);

                            clients.put(node.id(), client);
                        });
                    }

                    this.clientsTopology = newTopology;
                }
            }
        } finally {
            lock.unlockWrite(writeLock);
        }

        if (clientsToClose != null) {
            clientsToClose.forEach(MessagingClient::close);
        }
    }

    // This method is for testing purposes only.

    MessagingClient<T> clientOf(ClusterNodeId nodeId) throws MessagingException {
        // Ensure that we are using the latest topology.
        updateTopology();

        long readLock = lock.readLock();

        try {
            return clients.get(nodeId);
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private List<ClusterNode> nodesForBroadcast(Object affinityKey, MessagingOpts<T> opts) {
        List<ClusterNode> nodes;

        if (affinityKey == null) {
            // Use the whole topology if affinity key is not specified.
            nodes = opts.cluster().topology().nodes();
        } else {
            // Use only those nodes that are mapped to the partition.
            nodes = opts.partitions().map(affinityKey).nodes();
        }

        return nodes;
    }

    private void doScheduleTimeout(long timeout, MessageContext<T> ctx, Object callback) {
        if (!ctx.isCompleted()) {
            try {
                Future<?> future = ctx.worker().executeDeferred(timeout, () -> {
                    if (ctx.completeOnTimeout()) {
                        // Timed out.
                        T msg = ctx.originalMessage();

                        doNotifyOnError(callback, new MessageTimeoutException("Messaging operation timed out [message=" + msg + ']'));
                    } else {
                        // Try reschedule next timeout (for subscriptions).
                        doScheduleTimeout(timeout, ctx, callback);
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
            return clients.containsKey(node.id());
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private MessageAttempt<T> route(
        MessageContext<T> ctx,
        Router<T> router,
        Optional<FailureInfo> prevFailure
    ) throws HekateException, ClientSelectionRejectedException {
        // Perform routing in a loop to circumvent concurrent cluster topology changes.
        while (true) {
            PartitionMapper mapper = ctx.opts().partitions().snapshot();

            ClusterTopology topology = mapper.topology();

            // Fail if topology is empty.
            if (topology.isEmpty()) {
                if (prevFailure.isPresent()) {
                    throw new ClientSelectionRejectedException(prevFailure.get().error());
                } else {
                    throw new EmptyTopologyException("No suitable receivers [channel=" + name + ']');
                }
            }

            ClusterNodeId routed = router.route(ctx, prevFailure, mapper, topology, hekate);

            // Check if routing was successful.
            if (routed == null) {
                if (prevFailure.isPresent()) {
                    throw new ClientSelectionRejectedException(prevFailure.get().error());
                } else {
                    throw new UnknownRouteException("Load balancer failed to select a target node.");
                }
            }

            // Enter lock (prevents channel state changes).
            long readLock = lock.readLock();

            try {
                // Make sure that channel is not closed.
                if (closed) {
                    throw channelClosedError(null);
                }

                MessagingClient<T> client = clients.get(routed);

                if (client == null) {
                    // Post-check that topology was not changed during routing.
                    // -------------------------------------------------------------
                    // We are comparing the following topologies:
                    //  - Latest topology that is known to the cluster
                    //  - Topology that was used for routing (since it could expire while routing was in progress)
                    //  - Topology of client connections (since it is updated asynchronously and can lag behind the latest cluster topology)
                    // In case of any mismatch between those topologies we need to perform another routing attempt.
                    ClusterTopology latestTopology = ctx.opts().partitions().topology();

                    if (latestTopology.version() == topology.version()
                        && clientsTopology != null // <-- Can be null if service was still initializing when this method got called.
                        && clientsTopology.version() >= topology.version()) {
                        // Report failure since topologies are consistent but the selected node is not within the cluster.
                        if (prevFailure.isPresent()) {
                            throw new ClientSelectionRejectedException(prevFailure.get().error());
                        } else {
                            throw new UnknownRouteException("Node is not within the channel topology [id=" + routed + ']');
                        }
                    }

                    // Retry routing (note we are not exiting the loop)...
                } else {
                    return new MessageAttempt<>(client, topology, ctx, prevFailure);
                }
            } finally {
                lock.unlockRead(readLock);
            }

            // Since we are here it means that topology was changed during routing.
            if (debug) {
                log.debug("Retrying routing since topology was changed [balancer={}]", ctx.opts().balancer());
            }

            updateTopology();
        }
    }

    private void applyFailoverAsync(MessageAttempt<T> attempt, Throwable cause, FailoverCallback callback) {
        attempt.ctx().worker().execute(() ->
            applyFailover(attempt, cause, callback)
        );
    }

    private void applyFailover(MessageAttempt<T> attempt, Throwable cause, FailoverCallback callback) {
        // Do nothing if operation is already completed.
        if (attempt.ctx().isCompleted()) {
            return;
        }

        boolean applied = false;

        Throwable finalCause = cause;

        FailoverPolicy policy = attempt.ctx().opts().failover();

        if (policy != null && isRecoverable(cause)) {
            ClusterNode failedNode = attempt.client().node();

            DefaultFailoverContext failoverCtx = newFailoverContext(cause, failedNode, attempt.failure());

            // Apply failover policy.
            try {
                FailureResolution resolution = policy.apply(failoverCtx);

                // Enter lock (prevents channel state changes).
                long readLock = lock.readLock();

                try {
                    if (closed) {
                        finalCause = channelClosedError(cause);
                    } else if (resolution != null && resolution.isRetry()) {
                        FailoverRoutingPolicy routing = resolution.routing();

                        // Apply failover only if re-routing was requested or if the target node is still within the cluster topology.
                        if (routing != RETRY_SAME_NODE || clients.containsKey(failedNode.id())) {
                            onRetry();

                            MessagingWorker worker = attempt.ctx().worker();

                            // Schedule timeout task to apply failover actions after the failover delay.
                            worker.executeDeferred(resolution.delay(), () -> {
                                try {
                                    callback.retry(routing, Optional.of(failoverCtx.withRouting(routing)));
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

    private DefaultFailoverContext newFailoverContext(Throwable cause, ClusterNode failed, Optional<FailureInfo> prevFailure) {
        int attempt;
        FailoverRoutingPolicy prevRouting;
        Set<ClusterNode> failedNodes;

        if (prevFailure.isPresent()) {
            FailureInfo failure = prevFailure.get();

            attempt = failure.attempt() + 1;
            prevRouting = failure.routing();

            failedNodes = new HashSet<>(failure.allFailedNodes());

            failedNodes.add(failed);

            failedNodes = unmodifiableSet(failedNodes);
        } else {
            attempt = 0;
            prevRouting = RETRY_SAME_NODE;
            failedNodes = Collections.singleton(failed);
        }

        return new DefaultFailoverContext(attempt, cause, failed, failedNodes, prevRouting);
    }

    private boolean isRecoverable(Throwable cause) {
        return !(cause instanceof MessagingChannelClosedException)
            && !(cause instanceof CodecException);
    }

    private MessagingClient<T> createClient(ClusterNode node) {
        if (localNode.equals(node)) {
            return new MessagingClientMem<>(localNode, this);
        } else {
            return new MessagingClientNet<>(name, node, net, this, checkIdle);
        }
    }

    private long backPressureAcquire(long timeout, T msg) throws MessageQueueOverflowException, InterruptedException,
        MessageQueueTimeoutException {
        if (sendPressure != null) {
            return sendPressure.onEnqueue(timeout, msg);
        }

        return timeout;
    }

    private void backPressureRelease() {
        if (sendPressure != null) {
            sendPressure.onDequeue();
        }
    }

    private void onRetry() {
        metrics.onRetry();
    }

    private void notifyOnErrorAsync(MessageContext<T> ctx, Object callback, Throwable err) {
        ctx.worker().execute(() -> {
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
            } else if (callback instanceof ResponseCallback) {
                try {
                    ((ResponseCallback)callback).onComplete(err, null);
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected runtime error while notifying request callback on another error [cause={}]", err, e);
                }
            } else {
                throw new IllegalArgumentException("Unexpected callback type: " + callback);
            }
        }
    }

    private MessageContext<T> newContext(OutboundType type, Object affinityKey, T msg, MessagingOpts<T> opts) {
        int affinity = affinity(affinityKey);

        MessagingWorker worker;

        if (affinityKey != null
            || type == OutboundType.SUBSCRIBE /* <- Subscription operations should always be processed by the same thread. */) {
            worker = async.workerFor(affinity);
        } else {
            worker = async.pooledWorker();
        }

        return new MessageContext<>(msg, affinity, affinityKey, worker, opts, type, interceptor);
    }

    private MessagingChannelClosedException channelClosedError(Throwable cause) {
        return new MessagingChannelClosedException("Channel closed [channel=" + name + ']', cause);
    }

    private Throwable tryConvertToError(Response<T> reply, ClusterNode fromNode) {
        Throwable err = null;

        if (reply != null) {
            T replyMsg = reply.get();

            // Check if message should be converted to an error.
            if (replyMsg instanceof FailureResponse) {
                err = ((FailureResponse)replyMsg).asError(fromNode);

                if (err == null) {
                    err = new IllegalArgumentException(FailureResponse.class.getSimpleName() + " message returned null error "
                        + "[message=" + replyMsg + ']');
                }
            }
        }

        return err;
    }

    private void checkMessageType(T msg) {
        assert msg != null : "Message must be not null.";

        if (!baseType.isInstance(msg)) {
            throw new ClassCastException("Messaging channel doesn't support the specified type "
                + "[channel-type=" + baseType.getName() + ", message-type=" + msg.getClass().getName() + ']');
        }
    }

    private static int affinity(Object key) {
        if (key == null) {
            return ThreadLocalRandom.current().nextInt();
        } else {
            return key.hashCode();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Router<T> unicastRouter() {
        return (Router<T>)UNICAST_ROUTER;
    }

    private static boolean isFinalOrVoidResponse(Response<?> reply) {
        return reply == null || !reply.isPartial();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
