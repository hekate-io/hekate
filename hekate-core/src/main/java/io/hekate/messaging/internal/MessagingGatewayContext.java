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
import io.hekate.messaging.MessageInterceptor;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.broadcast.BroadcastResult;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.unicast.FailureResponse;
import io.hekate.messaging.unicast.RejectedReplyException;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.StreamFuture;
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
import java.util.LinkedList;
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
        void retry(FailoverRoutingPolicy routingPolicy, FailureInfo newFailure);

        void fail(Throwable cause);
    }

    private static class ClientSelectionRejectedException extends Exception {
        private static final long serialVersionUID = 1;

        public ClientSelectionRejectedException(Throwable cause) {
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

    private static class ResponseCallbackFuture<T> extends ResponseFuture<T> implements ResponseCallback<T> {
        @Override
        public void onComplete(Throwable err, Response<T> rsp) {
            if (err == null) {
                if (!rsp.isPartial()) {
                    complete(rsp);
                }
            } else {
                completeExceptionally(err);
            }
        }
    }

    private static class StreamCallbackFuture<T> extends StreamFuture<T> implements ResponseCallback<T> {
        private List<T> result;

        @Override
        public void onComplete(Throwable err, Response<T> rsp) {
            if (err == null) {
                // No need to synchronize since streams are always processed by the same thread.
                if (result == null) {
                    result = new ArrayList<>();
                }

                result.add(rsp.get());

                if (!rsp.isPartial()) {
                    complete(result);
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
    private final Set<MessagingConnectionNetIn<T>> inbound = new HashSet<>();

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
    private final int nioThreads;

    @ToStringIgnore
    private final MessageInterceptor<T> interceptor;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> channel;

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
        int nioThreads,
        MessagingExecutor async,
        MessagingMetrics metrics,
        ReceivePressureGuard receivePressure,
        SendPressureGuard sendPressure,
        MessageInterceptor<T> interceptor,
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
        this.nioThreads = nioThreads;
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

    public MessagingChannelId id() {
        return id;
    }

    public String name() {
        return name;
    }

    public int nioThreads() {
        return nioThreads;
    }

    public int workerThreads() {
        return async.poolSize();
    }

    public ClusterNode localNode() {
        return localNode;
    }

    public MessageInterceptor<T> interceptor() {
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
        assert msg != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";

        checkMessageType(msg);

        MessageContext<T> ctx = newContext(affinityKey, msg, opts);

        try {
            long timeout = backPressureAcquire(ctx.opts().timeout(), msg);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }

            routeAndSend(ctx, callback, null);
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        }
    }

    public ResponseFuture<T> request(Object affinityKey, T msg, MessagingOpts<T> opts) {
        ResponseCallbackFuture<T> future = new ResponseCallbackFuture<>();

        request(affinityKey, msg, opts, future);

        return future;
    }

    public void request(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        assert msg != null : "Request message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        checkMessageType(msg);

        MessageContext<T> ctx = newContext(affinityKey, msg, opts);

        requestAsync(msg, ctx, callback);
    }

    public StreamFuture<T> stream(Object affinityKey, T msg, MessagingOpts<T> opts) {
        StreamCallbackFuture<T> future = new StreamCallbackFuture<>();

        stream(affinityKey, msg, opts, future);

        return future;
    }

    public void stream(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        assert msg != null : "Request message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        checkMessageType(msg);

        MessageContext<T> ctx = newContext(affinityKey, msg, opts, true);

        requestAsync(msg, ctx, callback);
    }

    public BroadcastFuture<T> broadcast(Object affinityKey, T msg, MessagingOpts<T> opts) {
        BroadcastCallbackFuture<T> future = new BroadcastCallbackFuture<>();

        broadcast(affinityKey, msg, opts, future);

        return future;
    }

    public void broadcast(Object affinityKey, T msg, MessagingOpts<T> opts, BroadcastCallback<T> callback) {
        assert msg != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        checkMessageType(msg);

        Set<ClusterNode> nodes = opts.cluster().topology().nodeSet();

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyBroadcastResult<>(msg));
        } else {
            BroadcastContext<T> broadcast = new BroadcastContext<>(msg, nodes, callback);

            for (ClusterNode node : nodes) {
                send(affinityKey, msg, opts.forSingleNode(node), err -> {
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
        assert msg != null : "Message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        checkMessageType(msg);

        List<ClusterNode> nodes;

        if (affinityKey == null) {
            // Use the whole topology if affinity key is not specified.
            nodes = opts.cluster().topology().nodes();
        } else {
            // Use only nodes that are mapped to the partition.
            nodes = opts.partitions().map(affinityKey).nodes();
        }

        if (nodes.isEmpty()) {
            callback.onComplete(null, new EmptyAggregateResult<>(msg));
        } else {
            AggregateContext<T> aggregate = new AggregateContext<>(msg, nodes, callback);

            for (ClusterNode node : nodes) {
                request(affinityKey, msg, opts.forSingleNode(node), (err, reply) -> {
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
                List<NetworkFuture<MessagingProtocol>> disconnects = new LinkedList<>();

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

                disconnects.addAll(localInbound.stream()
                    .map(MessagingConnectionNetIn::disconnect)
                    .filter(Objects::nonNull)
                    .collect(toList()));

                waiting = new LinkedList<>();

                // Collect disconnect futures to waiting list.
                disconnects.stream()
                    .map(future -> (Waiting)future::join)
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

    private void routeAndSend(MessageContext<T> ctx, SendCallback callback, FailureInfo prevErr) {
        MessageRoute<T> route = null;

        try {
            route = route(ctx, prevErr);
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

        if (route != null) {
            doSend(route, callback, prevErr);
        }
    }

    private void doSend(MessageRoute<T> route, SendCallback callback, FailureInfo prevErr) {
        MessageContext<T> ctx = route.ctx();

        // Decorate callback with failover, error handling logic, etc.
        SendCallback retryCallback = err -> {
            route.client().touch();

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
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doSend(route, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(route.client().node())) {
                                    doSend(route, callback, failure);
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

                failoverAsync(ctx, err, route.client().node(), onFailover, prevErr);
            }
        };

        route.client().send(route, retryCallback, prevErr != null);
    }

    private void requestAsync(T request, MessageContext<T> ctx, ResponseCallback<T> callback) {
        try {
            long timeout = backPressureAcquire(ctx.opts().timeout(), request);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }

            routeAndRequest(ctx, callback, null);
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        }
    }

    private void routeAndRequest(MessageContext<T> ctx, ResponseCallback<T> callback, FailureInfo prevErr) {
        MessageRoute<T> route = null;

        try {
            route = route(ctx, prevErr);
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

        if (route != null) {
            doRequest(route, callback, prevErr);
        }
    }

    private void doRequest(MessageRoute<T> route, ResponseCallback<T> callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        final MessageContext<T> ctx = route.ctx();

        InternalRequestCallback<T> internalCallback = (request, err, reply) -> {
            route.client().touch();

            // Check if reply is an application-level error message.
            if (err == null) {
                err = tryConvertToError(reply.get(), route.receiver());
            }

            // Resolve effective reply.
            Response<T> effectiveReply = err == null ? reply : null;

            // Check if request callback can accept the response.
            ReplyDecision decision = callback.accept(err, effectiveReply);

            if (decision == null) {
                decision = ReplyDecision.DEFAULT;
            }

            FailoverPolicy failover = ctx.opts().failover();

            if (decision == ReplyDecision.COMPLETE || decision == ReplyDecision.DEFAULT && err == null || failover == null) {
                // Check if this is the final response or an error (ignore chunks).
                if (err != null || !reply.isPartial()) {
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
            } else {
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
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doRequest(route, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(route.client().node())) {
                                    doRequest(route, callback, failure);
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

                // Apply failover.
                failoverAsync(ctx, err, route.client().node(), onFailover, prevErr);
            }
        };

        if (ctx.isStream()) {
            route.client().stream(route, internalCallback, prevErr != null);
        } else {
            route.client().request(route, internalCallback, prevErr != null);
        }
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
        long readLock = lock.readLock();

        try {
            return clients.get(nodeId);
        } finally {
            lock.unlockRead(readLock);
        }
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
                        // Try reschedule next timeout (for streams).
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

    private MessageRoute<T> route(MessageContext<T> ctx, FailureInfo prevErr) throws HekateException, ClientSelectionRejectedException {
        assert ctx != null : "Message context is null.";

        while (true) {
            ClusterTopology topology = ctx.opts().cluster().topology();

            // Fail if topology is empty.
            if (topology.isEmpty()) {
                Throwable cause = prevErr != null ? prevErr.error() : null;

                if (cause == null) {
                    throw new EmptyTopologyException("No suitable receivers [channel=" + name + ']');
                } else {
                    throw new ClientSelectionRejectedException(cause);
                }
            }

            ClusterNodeId selected;

            if (ctx.opts().balancer() == null) {
                // Special case for broadcast/aggregate operations.
                // ---------------------------------------------------------------------------
                // Such operations do not specify a load balancer
                // but make sure that the target topology always contains only a single node.
                ClusterNode node = topology.first();

                selected = node != null ? node.id() : null;
            } else {
                // Route via load balancer.
                PartitionMapper partitions = ctx.opts().partitions();
                Optional<FailureInfo> failure = Optional.ofNullable(prevErr);
                LoadBalancerContext balancerCtx = new DefaultLoadBalancerContext(ctx, topology, hekate, partitions, failure);

                // Apply load balancer.
                selected = ctx.opts().balancer().route(ctx.originalMessage(), balancerCtx);
            }

            // Check if routing was successful.
            if (selected == null) {
                Throwable cause = prevErr != null ? prevErr.error() : null;

                if (cause == null) {
                    throw new UnknownRouteException("Load balancer failed to select a target node.");
                } else {
                    throw new ClientSelectionRejectedException(cause);
                }
            }

            // Enter lock (prevents channel state changes).
            long readLock = lock.readLock();

            try {
                // Make sure that channel is not closed.
                if (closed) {
                    throw channelClosedError(null);
                }

                MessagingClient<T> client = clients.get(selected);

                if (client == null) {
                    // Post-check that topology was not changed during routing.
                    ClusterTopology latestTopology = cluster.topology();

                    if (clientsTopology != null && clientsTopology.version() == latestTopology.version()) {
                        Throwable cause = prevErr != null ? prevErr.error() : null;

                        if (cause == null) {
                            throw new UnknownRouteException("Node is not within the channel topology [id=" + selected + ']');
                        } else {
                            throw new ClientSelectionRejectedException(cause);
                        }
                    }
                } else {
                    return new MessageRoute<>(client, topology, ctx, interceptor);
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

    private void failoverAsync(MessageContext<T> ctx, Throwable cause, ClusterNode failed, FailoverCallback callback, FailureInfo prevErr) {
        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            failover(ctx, cause, failed, callback, prevErr);
        });
    }

    private void failover(MessageContext<T> ctx, Throwable cause, ClusterNode failed, FailoverCallback callback, FailureInfo prevErr) {
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

        FailoverPolicy policy = ctx.opts().failover();

        if (policy != null && isRecoverable(cause)) {
            DefaultFailoverContext failoverCtx = newFailoverContext(cause, failed, prevErr);

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
                        if (routing != RETRY_SAME_NODE || clients.containsKey(failed.id())) {
                            onRetry();

                            MessagingWorker worker = ctx.worker();

                            // Schedule timeout task to apply failover actions after the failover delay.
                            onAsyncEnqueue();

                            worker.executeDeferred(resolution.delay(), () -> {
                                onAsyncDequeue();

                                try {
                                    callback.retry(routing, failoverCtx.withRouting(routing));
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

    private DefaultFailoverContext newFailoverContext(Throwable cause, ClusterNode failed, FailureInfo prevErr) {
        int attempt;
        FailoverRoutingPolicy prevRouting;
        Set<ClusterNode> failedNodes;

        if (prevErr == null) {
            attempt = 0;
            prevRouting = RETRY_SAME_NODE;
            failedNodes = Collections.singleton(failed);
        } else {
            attempt = prevErr.attempt() + 1;
            prevRouting = prevErr.routing();

            failedNodes = new HashSet<>(prevErr.allFailedNodes());

            failedNodes.add(failed);

            failedNodes = unmodifiableSet(failedNodes);
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

    private void notifyOnErrorAsync(MessageContext<T> ctx, Object callback, Throwable err) {
        onAsyncEnqueue();

        ctx.worker().execute(() -> {
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

    private MessageContext<T> newContext(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return newContext(affinityKey, msg, opts, false);
    }

    private MessageContext<T> newContext(Object affinityKey, T msg, MessagingOpts<T> opts, boolean stream) {
        int affinity = affinity(affinityKey);

        MessagingWorker worker;

        if (affinityKey != null || stream /* <- Stream operations should always be processed by the same thread. */) {
            worker = async.workerFor(affinity);
        } else {
            worker = async.pooledWorker();
        }

        return new MessageContext<>(msg, affinity, affinityKey, worker, opts, stream);
    }

    private MessagingChannelClosedException channelClosedError(Throwable cause) {
        return new MessagingChannelClosedException("Channel closed [channel=" + name + ']', cause);
    }

    private Throwable tryConvertToError(T replyMsg, ClusterNode fromNode) {
        Throwable err = null;

        // Check if message should be converted to an error.
        if (replyMsg instanceof FailureResponse) {
            err = ((FailureResponse)replyMsg).asError(fromNode);

            if (err == null) {
                err = new IllegalArgumentException(FailureResponse.class.getSimpleName() + " message returned null error "
                    + "[message=" + replyMsg + ']');
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

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
