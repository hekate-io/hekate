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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.Hekate;
import io.hekate.util.StateGuard;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterAcceptManager {
    private static final Logger log = LoggerFactory.getLogger(ClusterAcceptManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final StateGuard guard = new StateGuard(ClusterAcceptManager.class);

    private final Map<ClusterNodeId, CompletableFuture<Optional<String>>> activeChecks = new ConcurrentHashMap<>();

    private final List<ClusterAcceptor> acceptors;

    private final ExecutorService async;

    public ClusterAcceptManager(List<ClusterAcceptor> acceptors, ExecutorService async) {
        assert acceptors != null : "Acceptors list is null.";
        assert async != null : "Async worker is null.";

        this.async = async;

        this.acceptors = new ArrayList<>(acceptors);

        // Register default acceptor.
        this.acceptors.add((joining, hekate) -> {
            boolean locLoopback = hekate.localNode().socket().getAddress().isLoopbackAddress();
            boolean remLoopback = joining.socket().getAddress().isLoopbackAddress();

            if (locLoopback != remLoopback) {
                if (locLoopback) {
                    return "Cluster is configured with loopback addresses while node is configured to use a non-loopback address "
                        + "[rejected-by=" + hekate.localNode().address() + ']';
                } else {
                    return "Cluster is configured with non-loopback addresses while node is configured to use a loopback address "
                        + "[rejected-by=" + hekate.localNode().address() + ']';
                }
            }

            return null;
        });

        // Make sure that guard is in the 'initialized' state (must be done while write lock is being held).
        guard.withWriteLock(guard::becomeInitialized);
    }

    public void terminate() {
        guard.withWriteLock(() -> {
            if (guard.becomeTerminated()) {
                activeChecks.values().forEach(future -> future.cancel(false));

                activeChecks.clear();
            }
        });
    }

    public CompletableFuture<Optional<String>> check(ClusterNode joining, Hekate hekate) {
        guard.lockReadWithStateCheck();

        try {
            CompletableFuture<Optional<String>> future = new CompletableFuture<>();

            // Preserve enqueueing multiple checks for the same node.
            CompletableFuture<Optional<String>> existing = activeChecks.putIfAbsent(joining.id(), future);

            if (existing == null) {
                // Schedule new check.
                async.execute(() -> {
                    try {
                        guard.withReadLockIfInitialized(() -> {
                            String rejectReason = doAccept(joining, hekate);

                            future.complete(Optional.ofNullable(rejectReason));
                        });
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected error during the joining node acceptance testing [node={}]", joining, e);
                    } finally {
                        // Allow subsequent checks of this node.
                        activeChecks.remove(joining.id());
                    }
                });

                return future;
            } else {
                return existing;
            }
        } finally {
            guard.unlockRead();
        }
    }

    private String doAccept(ClusterNode newNode, Hekate hekate) {
        if (DEBUG) {
            log.debug("Checking the join acceptors [node={}]", newNode);
        }

        String rejectReason = null;

        for (ClusterAcceptor acceptor : acceptors) {
            rejectReason = acceptor.acceptJoin(newNode, hekate);

            if (rejectReason != null) {
                if (DEBUG) {
                    log.debug("Rejected cluster join request [node={}, reason={}, acceptor={}]", newNode, rejectReason, acceptor);
                }

                break;
            }
        }

        if (DEBUG) {
            if (rejectReason == null) {
                log.debug("New node accepted [node={}]", newNode);
            } else {
                log.debug("New node rejected [node={}, reason={}]", newNode, rejectReason);
            }
        }

        return rejectReason;
    }
}
