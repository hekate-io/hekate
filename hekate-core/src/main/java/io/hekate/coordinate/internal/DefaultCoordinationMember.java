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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.coordinate.CoordinationMember;
import io.hekate.coordinate.CoordinationRequestCallback;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.MessagingChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.newSetFromMap;

class DefaultCoordinationMember implements CoordinationMember {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationMember.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final Set<Future<?>> requests = newSetFromMap(new IdentityHashMap<>());

    private final Object mux = new Object();

    private final String process;

    private final ClusterNode node;

    private final ClusterTopology topology;

    private final boolean coordinator;

    private final MessagingChannel<CoordinationProtocol> channel;

    private final ExecutorService async;

    private final ClusterNode localNode;

    private volatile boolean disposed;

    public DefaultCoordinationMember(
        String process,
        ClusterNode node,
        ClusterTopology topology,
        boolean coordinator,
        MessagingChannel<CoordinationProtocol> channel,
        ExecutorService async
    ) {
        assert process != null : "Coordination process name is null.";
        assert node != null : "Node is null.";
        assert topology != null : "Topology is null.";
        assert channel != null : "Channel is null.";
        assert async != null : "Executor service is null.";

        this.process = process;
        this.localNode = topology.localNode();
        this.node = node;
        this.topology = topology;
        this.coordinator = coordinator;
        this.async = async;

        this.channel = channel.forNode(node);
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

        doRequest(new CoordinationProtocol.Request(process, from, clusterHash, request), callback);
    }

    public void sendPrepare(CoordinationRequestCallback callback) {
        ClusterNodeId from = localNode.id();
        ClusterHash clusterHash = topology.hash();

        doRequest(new CoordinationProtocol.Prepare(process, from, clusterHash), callback);
    }

    public void sendComplete(CoordinationRequestCallback callback) {
        ClusterNodeId from = localNode.id();
        ClusterHash clusterHash = topology.hash();

        doRequest(new CoordinationProtocol.Complete(process, from, clusterHash), callback);
    }

    public void dispose() {
        List<Future<?>> toCancel;

        synchronized (mux) {
            if (disposed) {
                toCancel = Collections.emptyList();
            } else {
                disposed = true;

                toCancel = new ArrayList<>(requests);
            }
        }

        if (!toCancel.isEmpty()) {
            toCancel.forEach(future ->
                future.cancel(false)
            );
        }
    }

    private void doRequest(CoordinationProtocol.RequestBase request, CoordinationRequestCallback callback) {
        if (DEBUG) {
            log.debug("Sending coordination request [to={}, message={}]", node, request);
        }

        CompletableFuture<Object> future = newRequestFuture(request, callback);

        if (!future.isDone()) {
            channel.newRequest(request)
                .withAffinity(process)
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .alwaysTrySameNode()
                    .whileTrue(() -> !disposed && !future.isDone())
                    .whileResponse(rsp -> {
                        if (rsp.is(CoordinationProtocol.Reject.class)) {
                            if (DEBUG) {
                                log.debug("Got a reject [from={}, request={}]", node, request);
                            }

                            return true;
                        } else {
                            if (rsp.is(CoordinationProtocol.Confirm.class)) {
                                if (DEBUG) {
                                    log.debug("Got a confirmation [from={}, request={}]", node, request);
                                }

                                future.complete(null);
                            } else {
                                CoordinationProtocol.Response response = rsp.payload(CoordinationProtocol.Response.class);

                                if (DEBUG) {
                                    log.debug("Got a response [from={}, response={}]", node, response.response());
                                }

                                future.complete(response.response());
                            }

                            return false;
                        }
                    })
                )
                .submit((err, rsp) -> {
                    unregister(future);

                    if (err != null) {
                        future.completeExceptionally(err);
                    }
                });
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
                        log.debug("Canceled coordination request [to={}, message={}]", node, req);
                    }

                    callback.onCancel();
                }
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error while notifying coordination request callback.", e);
            }
        }, async);

        return tryRegisterOrCancel(future);
    }

    private CompletableFuture<Object> tryRegisterOrCancel(CompletableFuture<Object> future) {
        if (!tryRegister(future)) {
            future.cancel(false);
        }

        return future;
    }

    private boolean tryRegister(CompletableFuture<Object> future) {
        synchronized (mux) {
            if (disposed) {
                return false;
            } else {
                requests.add(future);

                return true;
            }
        }
    }

    private void unregister(CompletableFuture<Object> future) {
        synchronized (mux) {
            requests.remove(future);
        }
    }

    @Override
    public String toString() {
        return node.toString();
    }
}
