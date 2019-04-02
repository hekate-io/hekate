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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.CodecException;
import io.hekate.core.HekateException;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.operation.FailureResponse;
import io.hekate.messaging.operation.RejectedResponseException;
import io.hekate.messaging.operation.Response;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.FixedBackoffPolicy;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.async.ExtendedScheduledExecutor;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import org.slf4j.Logger;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RETRY_SAME_NODE;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

class MessagingGatewayContext<T> {
    private interface RetryCallback {
        void retry(RetryRoutingPolicy routingPolicy, Optional<FailedAttempt> newFailure);

        void fail(Throwable cause);
    }

    private static class ClientSelectionRejectedException extends Exception {
        private static final long serialVersionUID = 1;

        public ClientSelectionRejectedException(Throwable cause) {
            super(null, cause, false, false);
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
    private final NetworkConnector<MessagingProtocol> net;

    @ToStringIgnore
    private final ClusterView cluster;

    @ToStringIgnore
    private final MessageReceiver<T> receiver;

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
    private final MessageInterceptors<T> interceptors;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> channel;

    @ToStringIgnore
    private final Set<MessagingConnectionIn<T>> inbound = new HashSet<>();

    @ToStringIgnore
    private final Map<ClusterNodeId, MessagingClient<T>> clients = new HashMap<>();

    @ToStringIgnore
    private final ExtendedScheduledExecutor timer;

    @ToStringIgnore
    private final long messagingTimeout;

    @ToStringIgnore
    private final GenericRetryConfigurer baseRetryPolicy;

    @ToStringIgnore
    private final int warnOnRetry;

    @ToStringIgnore
    private ClusterTopology clientsTopology;

    @ToStringIgnore
    private volatile boolean closed;

    public MessagingGatewayContext(
        String name,
        Class<T> baseType,
        NetworkConnector<MessagingProtocol> net,
        ClusterNode localNode,
        MessageReceiver<T> receiver,
        MessagingExecutor async,
        ExtendedScheduledExecutor timer,
        MessagingMetrics metrics,
        ReceivePressureGuard receivePressure,
        SendPressureGuard sendPressure,
        MessageInterceptors<T> interceptors,
        Logger log,
        boolean checkIdle,
        long messagingTimeout,
        int warnOnRetry,
        GenericRetryConfigurer baseRetryPolicy,
        DefaultMessagingChannel<T> channel
    ) {
        assert name != null : "Name is null.";
        assert baseType != null : "Base type is null.";
        assert net != null : "Network connector is null.";
        assert localNode != null : "Local cluster node is null.";
        assert async != null : "Executor is null.";
        assert metrics != null : "Metrics are null.";
        assert channel != null : "Default channel is null.";
        assert baseRetryPolicy != null : "Base retry policy is null.";

        this.id = new MessagingChannelId();
        this.name = name;
        this.baseType = baseType;
        this.net = net;
        this.localNode = localNode;
        this.cluster = channel.cluster();
        this.receiver = receiver;
        this.interceptors = interceptors;
        this.async = async;
        this.timer = timer;
        this.metrics = metrics;
        this.receivePressure = receivePressure;
        this.sendPressure = sendPressure;
        this.messagingTimeout = messagingTimeout;
        this.baseRetryPolicy = baseRetryPolicy;
        this.warnOnRetry = warnOnRetry;
        this.checkIdle = checkIdle;
        this.log = log;
        this.debug = log.isDebugEnabled();
        this.channel = channel;
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

    public MessageInterceptors<T> interceptors() {
        return interceptors;
    }

    public Logger log() {
        return log;
    }

    public long messagingTimeout() {
        return messagingTimeout;
    }

    public GenericRetryConfigurer baseRetryPolicy() {
        return baseRetryPolicy;
    }

    public MessageReceiver<T> receiver() {
        return receiver;
    }

    public Executor executor() {
        return async.pooledWorker();
    }

    public ClusterView cluster() {
        return cluster;
    }

    public void submit(MessageOperation<T> op) {
        checkMessageType(op.message());

        try {
            long remainingTimeout = applyBackPressure(op);

            if (op.hasTimeout()) {
                scheduleTimeout(op, remainingTimeout);
            }

            routeAndSubmit(op, Optional.empty());
        } catch (RejectedExecutionException e) {
            notifyOnErrorAsync(op, channelClosedError(null));
        } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
            notifyOnErrorAsync(op, e);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public Waiting close() {
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
                List<MessagingConnectionIn<T>> localInbound;

                synchronized (inbound) {
                    // Create a local copy of inbound connections since they are removing themselves from the list during disconnect.
                    localInbound = new ArrayList<>(inbound);

                    inbound.clear();
                }

                localInbound.stream()
                    .map(MessagingConnectionIn::disconnect)
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

    private void routeAndSubmit(MessageOperation<T> op, Optional<FailedAttempt> prevFailure) {
        MessageOperationAttempt<T> attempt = null;

        try {
            attempt = route(op, prevFailure);
        } catch (ClientSelectionRejectedException e) {
            notifyOnErrorAsync(op, e.getCause());
        } catch (HekateException e) {
            notifyOnErrorAsync(op, e);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during message routing.", e);
            }

            notifyOnErrorAsync(op, e);
        }

        if (attempt != null) {
            attempt.submit();
        }
    }

    private MessageOperationAttempt<T> route(MessageOperation<T> op, Optional<FailedAttempt> prevFailure) throws HekateException,
        ClientSelectionRejectedException {
        // Perform routing in a loop to circumvent concurrent cluster topology changes.
        while (true) {
            PartitionMapper mapperSnapshot = op.opts().partitions().snapshot();

            ClusterTopology topology = mapperSnapshot.topology();

            // Fail if topology is empty.
            if (topology.isEmpty()) {
                if (prevFailure.isPresent()) {
                    throw new ClientSelectionRejectedException(prevFailure.get().error());
                } else {
                    throw new EmptyTopologyException("No suitable receivers [channel=" + name + ']');
                }
            }

            ClusterNodeId routed = op.route(mapperSnapshot, prevFailure);

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
                    ClusterTopology latestTopology = op.opts().partitions().topology();

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
                    // Successful routing.
                    return createAttempt(op, prevFailure, topology, client);
                }
            } finally {
                lock.unlockRead(readLock);
            }

            // Since we are here it means that topology was changed during routing.
            if (debug) {
                log.debug("Retrying routing since topology was changed [balancer={}]", op.opts().balancer());
            }

            checkTopologyChanges();
        }
    }

    private MessageOperationAttempt<T> createAttempt(
        MessageOperation<T> operation,
        Optional<FailedAttempt> prevFailure,
        ClusterTopology topology,
        MessagingClient<T> client
    ) {
        MessageOperationCallback<T> callback = (attempt, rsp, err) -> {
            // Signal that network connection is not idle.
            attempt.client().touch();

            // Do not process completed operations.
            if (attempt.operation().isDone()) {
                return false;
            }

            // Check if reply is an application-level error.
            if (err == null) {
                err = tryConvertToError(rsp, attempt.receiver());
            }

            // Nullify response if it was converted to an error.
            ResponsePart<T> effectiveRsp = err == null ? rsp : null;

            boolean completed = false;

            if (shouldComplete(attempt, effectiveRsp, err)) {
                /////////////////////////////////////////////////////////////
                // Complete the operation.
                /////////////////////////////////////////////////////////////
                // Note that it is up to the operation to decide on whether it is really complete or not.
                completed = attempt.operation().complete(err, effectiveRsp);
            } else {
                /////////////////////////////////////////////////////////////
                // Retry.
                /////////////////////////////////////////////////////////////
                if (!attempt.operation().isDone()) {
                    // Complete the current attempt (successful retry actions will result in a new attempt).
                    completed = true;

                    RetryErrorPredicate policy;

                    // Check whether it was a real error or response was rejected by the user application logic.
                    if (err == null) {
                        err = new RejectedResponseException("Response rejected by the application logic", effectiveRsp.payload());

                        // Always retry.
                        policy = RetryErrorPredicate.acceptAll();
                    } else {
                        // Use operation's policy.
                        policy = attempt.operation().retryErrorPolicy();
                    }

                    // Retry callback.
                    RetryCallback onRetry = new RetryCallback() {
                        @Override
                        public void retry(RetryRoutingPolicy routing, Optional<FailedAttempt> failure) {
                            switch (routing) {
                                case RETRY_SAME_NODE: {
                                    attempt.nextAttempt(failure).submit();

                                    break;
                                }
                                case PREFER_SAME_NODE: {
                                    if (isKnownNode(attempt.receiver())) {
                                        attempt.nextAttempt(failure).submit();
                                    } else {
                                        routeAndSubmit(attempt.operation(), failure);
                                    }

                                    break;
                                }
                                case RE_ROUTE: {
                                    routeAndSubmit(attempt.operation(), failure);

                                    break;
                                }
                                default: {
                                    throw new IllegalArgumentException("Unexpected routing policy: " + routing);
                                }
                            }
                        }

                        @Override
                        public void fail(Throwable cause) {
                            notifyOnErrorAsync(attempt.operation(), cause);
                        }
                    };

                    // Retry.
                    retryAsync(attempt, policy, err, onRetry);
                }
            }

            return completed;
        };

        return new MessageOperationAttempt<>(client, topology, operation, prevFailure, callback);
    }

    private void retryAsync(MessageOperationAttempt<T> attempt, RetryErrorPredicate policy, Throwable cause, RetryCallback callback) {
        attempt.operation().worker().execute(() ->
            retry(attempt, policy, cause, callback)
        );
    }

    private void retry(MessageOperationAttempt<T> attempt, RetryErrorPredicate policy, Throwable cause, RetryCallback callback) {
        MessageOperation<T> operation = attempt.operation();

        // Do nothing if operation is already completed.
        if (operation.isDone()) {
            return;
        }

        boolean applied = false;

        Throwable finalCause = cause;

        if (policy != null && isRecoverable(cause)) {
            ClusterNode failedNode = attempt.receiver();

            MessageOperationFailure failure = newFailure(cause, failedNode, attempt.prevFailure());

            try {
                // Apply the retry policy.
                boolean shouldRetry = policy.shouldRetry(failure);

                if (shouldRetry) {
                    // Notify the operation that it will be retried.
                    operation.onRetry(failure);
                }

                // Enter lock (prevents channel state changes).
                long readLock = lock.readLock();

                try {
                    if (closed) {
                        finalCause = channelClosedError(cause);
                    } else if (shouldRetry) {
                        RetryRoutingPolicy routing = operation.retryRoute();

                        // Retry only if re-routing was requested or if the target node is still within the cluster topology.
                        if (routing != RETRY_SAME_NODE || clients.containsKey(failedNode.id())) {
                            metrics.onRetry();

                            // Prepare a retry task.
                            Runnable retry = () -> {
                                try {
                                    callback.retry(routing, Optional.of(failure.withRouting(routing)));
                                } catch (RuntimeException | Error e) {
                                    log.error("Got an unexpected error while retrying.", e);
                                }
                            };

                            // Calculate the retry delay.
                            long delay;

                            if (operation.retryBackoff() == null) {
                                // Use default policy.
                                delay = FixedBackoffPolicy.defaultPolicy().delayBeforeRetry(failure.attempt());
                            } else {
                                // Use custom policy.
                                delay = operation.retryBackoff().delayBeforeRetry(failure.attempt());
                            }

                            // Log a warning message (if configured).
                            if (shouldWarnOnRetry(failure)) {
                                if (log.isWarnEnabled()) {
                                    log.warn("Retrying messaging operation [attempt={}, delay={}, message={}]",
                                        failure.attempt(), delay, operation.message(), failure.error()
                                    );
                                }
                            }

                            // Schedule the retry task for asynchronous execution.
                            if (delay > 0) {
                                // Schedule timeout task to retry with the delay.
                                timer.schedule(() -> operation.worker().execute(retry), delay, TimeUnit.MILLISECONDS);
                            } else {
                                // Retry immediately.
                                operation.worker().execute(retry);
                            }

                            applied = true;
                        }
                    }
                } finally {
                    lock.unlockRead(readLock);
                }
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected error while retrying.", e);
            }
        }

        if (!applied) {
            callback.fail(finalCause);
        }
    }

    private boolean shouldWarnOnRetry(MessageOperationFailure failure) {
        return (warnOnRetry == 0)
            || (warnOnRetry > 0 && failure.attempt() > 0 && failure.attempt() % warnOnRetry == 0);
    }

    private long applyBackPressure(MessageOperation<T> op) throws MessageQueueOverflowException, InterruptedException,
        MessageQueueTimeoutException {

        if (sendPressure != null) {
            long remainingTime = sendPressure.onEnqueue(op.timeout(), op.message());

            op.registerSendPressure(sendPressure);

            return remainingTime;
        }

        return op.timeout();
    }

    private void scheduleTimeout(MessageOperation<T> op, long initTimeout) {
        assert initTimeout > 0 : "Timeout must be greater than zero [timeout=" + initTimeout + ']';

        Future<?> timeoutFuture = timer.repeatWithFixedDelay(() -> {
            if (op.isDone()) {
                // Do not execute anymore (operation already completed).
                return false;
            }

            if (op.shouldExpireOnTimeout()) {
                // Process expiration on the worker thread.
                op.worker().execute(() -> {
                    String errMsg = "Messaging operation timed out [timeout=" + op.timeout() + ", message=" + op.message() + ']';

                    doNotifyOnError(op, new MessageTimeoutException(errMsg));
                });

                // Do not execute anymore (operation timed out).
                return false;
            }

            // Re-run this check later (for subscriptions).
            return true;
        }, initTimeout, op.timeout(), TimeUnit.MILLISECONDS);

        op.registerTimeout(timeoutFuture);
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

    boolean register(MessagingConnectionIn<T> conn) {
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

    void unregister(MessagingConnectionIn<T> conn) {
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

    void checkTopologyChanges() {
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
        checkTopologyChanges();

        long readLock = lock.readLock();

        try {
            return clients.get(nodeId);
        } finally {
            lock.unlockRead(readLock);
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

    private MessageOperationFailure newFailure(Throwable cause, ClusterNode failed, Optional<FailedAttempt> prevFailure) {
        int attempt;
        RetryRoutingPolicy prevRouting;
        Set<ClusterNode> failedNodes;

        if (prevFailure.isPresent()) {
            FailedAttempt failure = prevFailure.get();

            attempt = failure.attempt() + 1;
            prevRouting = failure.routing();

            failedNodes = new HashSet<>(failure.allTriedNodes());

            failedNodes.add(failed);

            failedNodes = unmodifiableSet(failedNodes);
        } else {
            attempt = 0;
            prevRouting = RETRY_SAME_NODE;
            failedNodes = singleton(failed);
        }

        return new MessageOperationFailure(attempt, cause, failed, failedNodes, prevRouting);
    }

    private boolean isRecoverable(Throwable cause) {
        return !(cause instanceof MessagingChannelClosedException)
            && !(cause instanceof CodecException);
    }

    private MessagingClient<T> createClient(ClusterNode node) {
        return new MessagingClient<>(node, net, this, checkIdle);
    }

    private void notifyOnErrorAsync(MessageOperation<T> op, Throwable err) {
        op.worker().execute(() ->
            doNotifyOnError(op, err)
        );
    }

    private void doNotifyOnError(MessageOperation<T> op, Throwable err) {
        try {
            op.complete(err, null);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying on another error [cause={}]", err, e);
        }
    }

    private Throwable tryConvertToError(Response<T> rsp, ClusterNode from) {
        Throwable err = null;

        if (rsp != null) {
            T replyMsg = rsp.payload();

            // Check if message should be converted to an error.
            if (replyMsg instanceof FailureResponse) {
                err = ((FailureResponse)replyMsg).asError(from);

                if (err == null) {
                    err = new IllegalArgumentException(FailureResponse.class.getSimpleName() + " message returned null error "
                        + "[message=" + replyMsg + ']');
                }
            }
        }

        return err;
    }

    private MessagingChannelClosedException channelClosedError(Throwable cause) {
        return new MessagingChannelClosedException("Channel closed [channel=" + name + ']', cause);
    }

    private void checkMessageType(T msg) {
        assert msg != null : "Message must be not null.";

        if (!baseType.isInstance(msg)) {
            throw new ClassCastException("Messaging channel doesn't support the specified type "
                + "[channel-type=" + baseType.getName() + ", message-type=" + msg.getClass().getName() + ']');
        }
    }

    private boolean shouldComplete(MessageOperationAttempt<T> attempt, ResponsePart<T> rsp, Throwable err) {
        // Check if the operation can be retried at all.
        boolean canRetry = attempt.operation().canRetry();

        if (canRetry) {
            if (err == null) {
                // In case of a successful response we need to check if the user application accepts it.
                if (rsp == null || !attempt.operation().shouldRetry(rsp)) {
                    // Retry not needed -> complete the operation.
                    return true;
                }
            } else {
                // In case of an error we need to check if a retry policy is set for the message operation.
                if (attempt.operation().retryErrorPolicy() == null) {
                    // No error retry policy -> complete the operation.
                    return true;
                }
            }

            // No more retry attempts -> complete the operation.
            return !attempt.hasMoreAttempts();
        } else {
            // Can't retry at all -> complete the operation.
            return true;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
