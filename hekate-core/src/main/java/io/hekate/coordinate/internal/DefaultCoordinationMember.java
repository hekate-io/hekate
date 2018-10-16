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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.coordinate.CoordinationMember;
import io.hekate.coordinate.CoordinationRequestCallback;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.messaging.unicast.ReplyDecision.ACCEPT;
import static io.hekate.messaging.unicast.ReplyDecision.REJECT;

class DefaultCoordinationMember implements CoordinationMember {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationMember.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String processName;

    private final ClusterNode node;

    private final ClusterTopology topology;

    private final boolean coordinator;

    private final MessagingChannel<CoordinationProtocol> channel;

    private final ExecutorService async;

    private final ReentrantLock lock = new ReentrantLock();

    private final Set<Future<?>> requestFutures = Collections.newSetFromMap(new IdentityHashMap<>());

    private final ClusterNode localNode;

    private volatile boolean disposed;

    public DefaultCoordinationMember(
        String processName,
        ClusterNode node,
        ClusterTopology topology,
        boolean coordinator,
        MessagingChannel<CoordinationProtocol> channel,
        ExecutorService async,
        long failoverDelay
    ) {
        assert processName != null : "Coordination process name is null.";
        assert node != null : "Node is null.";
        assert topology != null : "Topology is null.";
        assert channel != null : "Channel is null.";
        assert async != null : "Executor service is null.";

        this.processName = processName;
        this.localNode = topology.localNode();
        this.node = node;
        this.topology = topology;
        this.coordinator = coordinator;
        this.async = async;

        this.channel = channel.forNode(node)
            .withFailover(new FailoverPolicyBuilder()
                // Retry to death.
                .withRetryUntil(ctx -> !disposed)
                .withAlwaysRetrySameNode()
                .withConstantRetryDelay(failoverDelay)
            );
    }

    @Override
    public boolean isCoordinator() {
        return coordinator;
    }

    @Override
    public ClusterNode node() {
        return node;
    }

    @Override
    public void request(Object request, CoordinationRequestCallback callback) {
        ArgAssert.notNull(request, "Request");
        ArgAssert.notNull(callback, "Callback");

        ClusterNodeId from = localNode.id();
        ClusterHash clusterHash = topology.hash();

        doRequest(new CoordinationProtocol.Request(processName, from, clusterHash, request), callback);
    }

    public void sendPrepare(CoordinationRequestCallback callback) {
        ClusterNodeId from = localNode.id();
        ClusterHash clusterHash = topology.hash();

        doRequest(new CoordinationProtocol.Prepare(processName, from, clusterHash), callback);
    }

    public void sendComplete(CoordinationRequestCallback callback) {
        ClusterNodeId from = localNode.id();
        ClusterHash clusterHash = topology.hash();

        doRequest(new CoordinationProtocol.Complete(processName, from, clusterHash), callback);
    }

    public void dispose() {
        List<Future<?>> toCancel;

        lock.lock();

        try {
            if (disposed) {
                toCancel = Collections.emptyList();
            } else {
                disposed = true;

                toCancel = new ArrayList<>(requestFutures);
            }
        } finally {
            lock.unlock();
        }

        if (!toCancel.isEmpty()) {
            toCancel.forEach(future -> future.cancel(false));
        }
    }

    private void doRequest(CoordinationProtocol.RequestBase request, CoordinationRequestCallback callback) {
        if (DEBUG) {
            log.debug("Sending coordination request [to={}, message={}]", node, request);
        }

        CompletableFuture<Object> future = newRequestFuture(request, callback);

        boolean enqueued = false;

        lock.lock();

        try {
            if (!disposed) {
                enqueued = true;

                requestFutures.add(future);
            }
        } finally {
            lock.unlock();
        }

        if (enqueued) {
            channel.request(request)
                .withAffinity(processName)
                .until((err, rsp) -> {
                    if (future.isDone()) {
                        if (DEBUG) {
                            log.debug("Skipped response [from={}, response={}]", node, rsp);
                        }

                        return ACCEPT;
                    } else if (err != null) {
                        if (DEBUG) {
                            log.debug("Got an error [from={}, error={}, request={}]", node, err.toString(), request);
                        }

                        return REJECT;
                    } else if (rsp.is(CoordinationProtocol.Reject.class)) {
                        if (DEBUG) {
                            log.debug("Got a reject [from={}, request={}]", node, request);
                        }

                        return REJECT;
                    } else {
                        if (rsp.is(CoordinationProtocol.Confirm.class)) {
                            if (DEBUG) {
                                log.debug("Got a confirmation [from={}, request={}]", node, request);
                            }

                            future.complete(null);
                        } else {
                            CoordinationProtocol.Response response = rsp.get(CoordinationProtocol.Response.class);

                            if (DEBUG) {
                                log.debug("Got a response [from={}, response={}]", node, response.response());
                            }

                            future.complete(response.response());
                        }

                        return ACCEPT;
                    }
                })
                .submit((err, rsp) -> {
                    unregister(future);

                    if (err != null && !disposed) {
                        if (DEBUG) {
                            log.debug("Failed to submit coordination request [request={}]", request, err);
                        }
                    }
                });
        } else {
            future.cancel(false);
        }
    }

    private CompletableFuture<Object> newRequestFuture(CoordinationProtocol.RequestBase req, CoordinationRequestCallback callback) {
        CompletableFuture<Object> future = new CompletableFuture<>();

        future.whenCompleteAsync((rsp, err) -> {
            try {
                if (err == null) {
                    if (DEBUG) {
                        log.debug("Received coordination response [from={}, message={}]", node, rsp);
                    }

                    callback.onResponse(rsp, this);
                } else {
                    if (DEBUG) {
                        log.debug("Canceled coordination request sending [to={}, message={}]", node, req);
                    }

                    callback.onCancel();
                }
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error while notifying coordination request callback.", e);
            }
        }, async);

        return future;
    }

    private void unregister(CompletableFuture<Object> future) {
        lock.lock();

        try {
            requestFutures.remove(future);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return node.toString();
    }
}
