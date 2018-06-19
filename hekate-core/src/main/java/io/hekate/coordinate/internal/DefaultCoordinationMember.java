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
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
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

import static io.hekate.messaging.unicast.ReplyDecision.COMPLETE;
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
            .withAffinity(processName)
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
            ClusterNodeId from = localNode.id();
            ClusterHash clusterHash = topology.hash();

            CoordinationProtocol.Request req = new CoordinationProtocol.Request(processName, from, clusterHash, request);

            channel.request(req, new ResponseCallback<CoordinationProtocol>() {
                @Override
                public ReplyDecision accept(Throwable err, Response<CoordinationProtocol> reply) {
                    if (future.isDone()) {
                        return COMPLETE;
                    } else if (err != null || reply.is(CoordinationProtocol.Reject.class)) {
                        return REJECT;
                    } else {
                        CoordinationProtocol.Response response = reply.get(CoordinationProtocol.Response.class);

                        future.complete(response.response());

                        return COMPLETE;
                    }
                }

                @Override
                public void onComplete(Throwable err, Response<CoordinationProtocol> rsp) {
                    unregister(future);

                    if (DEBUG) {
                        if (err != null && !disposed) {
                            log.debug("Failed to submit coordination request [request={}]", request, err);
                        }
                    }
                }
            });
        } else {
            future.cancel(false);
        }
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

    private CompletableFuture<Object> newRequestFuture(Object request, CoordinationRequestCallback callback) {
        CompletableFuture<Object> future = new CompletableFuture<>();

        future.whenCompleteAsync((response, error) -> {
            try {
                if (error == null) {
                    if (DEBUG) {
                        log.debug("Received coordination response [from={}, message={}]", node, response);
                    }

                    callback.onResponse(response, this);
                } else {
                    if (DEBUG) {
                        log.debug("Canceled coordination request sending [to={}, message={}]", node, request);
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
