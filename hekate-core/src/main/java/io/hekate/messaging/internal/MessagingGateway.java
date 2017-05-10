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
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.ReplyFailure;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.partition.Partition;
import io.hekate.partition.RendezvousHashMapper;
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
import org.slf4j.LoggerFactory;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static io.hekate.failover.FailoverRoutingPolicy.RE_ROUTE;
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

    private static class ClientSelectionRejectedException extends Exception {
        private static final long serialVersionUID = 1L;

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

    private static class SubscribeCallbackFuture<T> extends SubscribeFuture<T> implements ResponseCallback<T> {
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
    private final ClusterView cluster;

    @ToStringIgnore
    private final MessageReceiver<T> receiver;

    @ToStringIgnore
    private final CloseCallback onBeforeClose;

    @ToStringIgnore
    private final boolean checkIdle;

    @ToStringIgnore
    private final Set<NetworkInboundConnection<T>> inbound = new HashSet<>();

    @ToStringIgnore
    private final StampedLock lock = new StampedLock();

    @ToStringIgnore
    private final MessagingExecutor async;

    @ToStringIgnore
    private final MetricsCallback metrics;

    @ToStringIgnore
    private final ReceivePressureGuard receivePressure;

    @ToStringIgnore
    private final SendPressureGuard sendPressure;

    @ToStringIgnore
    private final int nioThreads;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> channel;

    @ToStringIgnore
    private final Map<ClusterNodeId, MessagingClient<T>> clients = new HashMap<>();

    @ToStringIgnore
    private ClusterTopology clientsTopology;

    @ToStringIgnore
    private boolean closed;

    public MessagingGateway(String name, NetworkConnector<MessagingProtocol> net, ClusterNode localNode, ClusterView cluster,
        MessageReceiver<T> receiver, int nioThreads, MessagingExecutor async, MetricsCallback metrics,
        ReceivePressureGuard receivePressure, SendPressureGuard sendPressure, FailoverPolicy failoverPolicy, long defaultTimeout,
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
        this.cluster = cluster;
        this.receiver = receiver;
        this.nioThreads = nioThreads;
        this.async = async;
        this.metrics = metrics;
        this.receivePressure = receivePressure;
        this.sendPressure = sendPressure;
        this.checkIdle = checkIdle;
        this.onBeforeClose = onBeforeClose;

        RendezvousHashMapper partitions = RendezvousHashMapper.of(cluster)
            .withBackupNodes(0)
            .build();

        this.channel = new DefaultMessagingChannel<>(this, cluster, partitions, loadBalancer, failoverPolicy, defaultTimeout, null);
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
            long timeout = backPressureAcquire(ctx.opts().timeout(), message);

            routeAndSend(ctx, callback, null);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        }
    }

    @SuppressWarnings("unchecked") // <- need to cast to the response type.
    public <R extends T> ResponseFuture<R> request(Object affinityKey, T message, MessagingOpts<T> opts) {
        ResponseCallbackFuture<T> future = new ResponseCallbackFuture<>();

        request(affinityKey, message, opts, future);

        return (ResponseFuture<R>)future;
    }

    public void request(Object affinityKey, T request, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        assert request != null : "Request message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, request, opts);

        requestAsync(request, ctx, callback);
    }

    @SuppressWarnings("unchecked") // <- need to cast to the response type.
    public <R extends T> SubscribeFuture<R> subscribe(Object affinityKey, T request, MessagingOpts<T> opts) {
        SubscribeCallbackFuture<T> future = new SubscribeCallbackFuture<>();

        subscribe(affinityKey, request, opts, future);

        return (SubscribeFuture<R>)future;
    }

    public void subscribe(Object affinityKey, T request, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        assert request != null : "Request message must be not null.";
        assert opts != null : "Messaging options must be not null.";
        assert callback != null : "Callback must be not null.";

        MessageContext<T> ctx = newContext(affinityKey, request, opts, true);

        requestAsync(request, ctx, callback);
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

        Set<ClusterNode> nodes = opts.cluster().topology().nodeSet();

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

        List<ClusterNode> nodes = opts.cluster().topology().nodes();

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
                        // It may happen that in some rare cases the node leaves the cluster at the same time with this operation.
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
                if (DEBUG) {
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
                List<NetworkInboundConnection<T>> localInbound;

                synchronized (inbound) {
                    // Create a local copy of inbound connections since they are removing themselves from the list during disconnect.
                    localInbound = new ArrayList<>(inbound);

                    inbound.clear();
                }

                disconnects.addAll(localInbound.stream()
                    .map(NetworkInboundConnection::disconnect)
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

    DefaultMessagingChannel<T> channel() {
        return channel;
    }

    MessagingExecutor async() {
        return async;
    }

    MetricsCallback metrics() {
        return metrics;
    }

    ReceivePressureGuard receiveGuard() {
        return receivePressure;
    }

    SendPressureGuard sendGuard() {
        return sendPressure;
    }

    private void routeAndSend(MessageContext<T> ctx, SendCallback callback, FailureInfo prevErr) {
        MessagingClient<T> client = null;

        try {
            client = selectClient(ctx, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (ClientSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (client != null) {
            doSend(ctx, client, callback, prevErr);
        }
    }

    private void doSend(MessageContext<T> ctx, MessagingClient<T> client, SendCallback callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        SendCallback retryCallback = err -> {
            client.touch();

            if (err == null || ctx.opts().failover() == null) {
                // Complete operation.
                if (ctx.complete()) {
                    backPressureRelease();

                    if (callback != null) {
                        callback.onComplete(err);
                    }
                }
            } else {
                // TODO: Urgent!!! Failover in MessagingGateway#send(...) is not covered by unit tests.
                // Apply failover actions.
                FailoverCallback onFailover = new FailoverCallback() {
                    @Override
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doSend(ctx, client, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(client.node())) {
                                    doSend(ctx, client, callback, failure);
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

                failoverAsync(ctx, err, client.node(), onFailover, prevErr);
            }
        };

        client.send(ctx, retryCallback, prevErr != null);
    }

    private void requestAsync(T request, MessageContext<T> ctx, ResponseCallback<T> callback) {
        try {
            long timeout = backPressureAcquire(ctx.opts().timeout(), request);

            routeAndRequest(ctx, callback, null);

            if (timeout > 0) {
                doScheduleTimeout(timeout, ctx, callback);
            }
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(ctx, callback, e);
        }
    }

    private void routeAndRequest(MessageContext<T> ctx, ResponseCallback<T> callback, FailureInfo prevErr) {
        MessagingClient<T> client = null;

        try {
            client = selectClient(ctx, prevErr);
        } catch (HekateException | RuntimeException | Error e) {
            notifyOnErrorAsync(ctx, callback, e);
        } catch (ClientSelectionRejectedException e) {
            notifyOnErrorAsync(ctx, callback, e.getCause());
        }

        if (client != null) {
            doRequest(ctx, client, callback, prevErr);
        }
    }

    private void doRequest(MessageContext<T> ctx, MessagingClient<T> client, ResponseCallback<T> callback, FailureInfo prevErr) {
        // Decorate callback with failover, error handling logic, etc.
        InternalRequestCallback<T> internalCallback = (request, err, reply) -> {
            client.touch();

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

            FailoverPolicy failover = ctx.opts().failover();

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
                    err = new RejectedReplyException("Response was rejected by a request callback", replyMsg, err);
                }

                FailoverCallback onFailover = new FailoverCallback() {
                    @Override
                    public void retry(FailoverRoutingPolicy routing, FailureInfo failure) {
                        switch (routing) {
                            case RETRY_SAME_NODE: {
                                doRequest(ctx, client, callback, failure);

                                break;
                            }
                            case PREFER_SAME_NODE: {
                                if (isKnownNode(client.node())) {
                                    doRequest(ctx, client, callback, failure);
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

                failoverAsync(ctx, err, client.node(), onFailover, prevErr);
            }
        };

        if (ctx.isSubscribe()) {
            client.subscribe(ctx, internalCallback, prevErr != null);
        } else {
            client.request(ctx, internalCallback, prevErr != null);
        }
    }

    boolean register(NetworkInboundConnection<T> conn) {
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

    void unregister(NetworkInboundConnection<T> conn) {
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
                    if (DEBUG) {
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

    void scheduleTimeout(MessageContext<T> ctx, Object callback) {
        doScheduleTimeout(ctx.opts().timeout(), ctx, callback);
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
                        T msg = ctx.message();

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
            return clients.containsKey(node.id());
        } finally {
            lock.unlockRead(readLock);
        }
    }

    private MessagingClient<T> selectClient(MessageContext<T> ctx, FailureInfo prevErr) throws HekateException,
        ClientSelectionRejectedException {
        assert ctx != null : "Message context is null.";

        while (true) {
            Partition partition = null;

            // Topology of this routing attempt.
            ClusterTopology topology;

            // Decide on whether we should use partition-based routing.
            if (ctx.opts().balancer() == null && ctx.hasAffinity()) {
                // Memorize partition so that we could re-use it later after all sanity checks.
                partition = ctx.opts().partitions().mapInt(ctx.affinity());

                topology = partition.topology();
            } else {
                topology = ctx.opts().cluster().topology();
            }

            // Fail if topology is empty.
            if (topology.isEmpty()) {
                Throwable cause = prevErr != null ? prevErr.error() : null;

                if (cause == null) {
                    throw new UnknownRouteException("No suitable receivers [channel=" + name + ']');
                } else {
                    throw new ClientSelectionRejectedException(cause);
                }
            }

            ClusterNodeId selected;

            // Check if load balancer is specified for this message.
            if (ctx.opts().balancer() == null) {
                // Load balancer not specified -> use default routing.
                ClusterNode node;

                if (partition == null) {
                    // Select any random node if affinity is not specified.
                    node = topology.random();

                    // Check if this is a failover attempt and try to re-route in case if the selected node is known to be failed.
                    if (prevErr != null && prevErr.routing() == RE_ROUTE && prevErr.isFailed(node)) {
                        // Exclude all failed nodes.
                        List<ClusterNode> nonFailed = topology.stream()
                            .filter(n -> !prevErr.isFailed(n))
                            .collect(toList());

                        if (!nonFailed.isEmpty()) {
                            // Randomize.
                            Collections.shuffle(nonFailed);

                            node = nonFailed.get(0);
                        }
                    }
                } else {
                    // Select node from the partition.
                    node = partition.primaryNode();

                    // Check if this is a failover attempt and try to re-route in case if the selected node is known to be failed.
                    if (prevErr != null && prevErr.routing() == RE_ROUTE && partition.hasBackupNodes() && prevErr.isFailed(node)) {
                        node = partition.backupNodes().stream()
                            .filter(n -> !prevErr.isFailed(n))
                            .findFirst()
                            .orElse(node); // <-- Fall back to the originally selected node.
                    }
                }

                selected = node != null ? node.id() : null;
            } else {
                // Use load balancer.
                LoadBalancerContext balancerCtx = new DefaultLoadBalancerContext(ctx, topology, Optional.ofNullable(prevErr));

                selected = ctx.opts().balancer().route(ctx.message(), balancerCtx);
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
                // Check that channel is not closed.
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
                    return client;
                }
            } finally {
                lock.unlockRead(readLock);
            }

            // Since we are here it means that topology was changed during routing.
            if (DEBUG) {
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
            return new InMemoryMessagingClient<>(localNode, this);
        } else {
            return new NetworkMessagingClient<>(name, node, net, this, checkIdle);
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

    private MessageContext<T> newContext(Object affinityKey, T message, MessagingOpts<T> opts) {
        return newContext(affinityKey, message, opts, false);
    }

    private MessageContext<T> newContext(Object affinityKey, T message, MessagingOpts<T> opts, boolean stream) {
        int affinity = affinity(affinityKey);

        MessagingWorker worker;

        if (affinityKey != null || stream /* <- Stream operations should always be processed by the same thread. */) {
            worker = async.workerFor(affinity);
        } else {
            worker = async.pooledWorker();
        }

        return new MessageContext<>(message, affinity, affinityKey, worker, opts, stream);
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
