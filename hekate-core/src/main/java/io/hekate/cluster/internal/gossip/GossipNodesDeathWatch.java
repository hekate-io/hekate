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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

public class GossipNodesDeathWatch {
    private static class Suspect {
        private final ClusterNodeId id;

        private final Set<ClusterNodeId> suspectedBy;

        private final long timestamp;

        public Suspect(ClusterNodeId id, Set<ClusterNodeId> suspectedBy, long timestamp) {
            assert id != null : "Node id is null.";
            assert suspectedBy != null : "Suspected by nodes list is null.";
            assert !suspectedBy.isEmpty() : "Suspected by nodes list is empty.";

            this.id = id;
            this.suspectedBy = suspectedBy;
            this.timestamp = timestamp;
        }

        public ClusterNodeId id() {
            return id;
        }

        public long timestamp() {
            return timestamp;
        }

        public boolean isSameSuspectedBy(Set<ClusterNodeId> other) {
            return suspectedBy.equals(other);
        }

        public boolean isDead(Set<ClusterNodeId> liveNodes, int quorum) {
            int realQuorum = Math.min(liveNodes.size(), quorum);

            int liveSuspected = 0;

            for (ClusterNodeId node : suspectedBy) {
                if (liveNodes.contains(node)) {
                    liveSuspected++;
                }
            }

            return liveSuspected >= realQuorum;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GossipNodesDeathWatch.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final ClusterNodeId localNodeId;

    private final long maxFailedNodeTimeout;

    private final int quorumSize;

    private final Map<ClusterNodeId, Suspect> suspects = new HashMap<>();

    private Set<ClusterNodeId> liveNodes = Collections.emptySet();

    public GossipNodesDeathWatch(ClusterNodeId localNodeId, int quorumSize, long maxFailedNodeTimeout) {
        assert localNodeId != null : "Local node id is null.";
        assert quorumSize > 0 : "Quorum size must be above zero.";

        this.localNodeId = localNodeId;
        this.maxFailedNodeTimeout = TimeUnit.MILLISECONDS.toNanos(maxFailedNodeTimeout);
        this.quorumSize = quorumSize;
    }

    public void update(Gossip gossip) {
        assert gossip != null : "Gossip is null.";

        GossipSuspectView suspectView = gossip.suspectView();

        Set<ClusterNodeId> newSuspects = suspectView.suspected().stream()
            .filter(suspectId -> !localNodeId.equals(suspectId) && gossip.hasMember(suspectId))
            .collect(toSet());

        Set<ClusterNodeId> newLiveNodes = new HashSet<>();

        gossip.stream()
            .map(GossipNodeState::id)
            .filter(id -> !newSuspects.contains(id) || !suspectView.suspecting(id).contains(localNodeId))
            .forEach(newLiveNodes::add);

        newLiveNodes.add(localNodeId);

        this.liveNodes = newLiveNodes;

        if (DEBUG) {
            if (newSuspects.isEmpty()) {
                log.trace("Updating suspects list [suspects={}]", newSuspects);
            } else {
                log.debug("Updating suspects list [suspects={}]", newSuspects);
            }
        }

        for (Iterator<Suspect> it = suspects.values().iterator(); it.hasNext(); ) {
            Suspect suspect = it.next();

            if (!newSuspects.contains(suspect.id())) {
                if (DEBUG) {
                    log.debug("Node is not suspected anymore [suspect={}]", suspect);
                }

                it.remove();
            }
        }

        long now = System.nanoTime();

        for (ClusterNodeId suspectId : newSuspects) {
            Suspect existing = suspects.get(suspectId);

            Set<ClusterNodeId> suspectedBy = suspectView.suspecting(suspectId);

            Suspect newSuspect = null;

            if (existing == null) {
                newSuspect = new Suspect(suspectId, suspectedBy, now);

                if (DEBUG) {
                    log.debug("Registering new suspect [suspect={}]", newSuspect);
                }
            } else if (!existing.isSameSuspectedBy(suspectedBy)) {
                newSuspect = new Suspect(suspectId, suspectedBy, now);

                if (DEBUG) {
                    log.debug("Updated existing suspect [new={}, old={}]", newSuspect, existing);
                }
            }

            if (newSuspect != null) {
                suspects.put(suspectId, newSuspect);
            }
        }
    }

    public List<ClusterNodeId> terminateNodes() {
        List<ClusterNodeId> terminated = null;

        if (!suspects.isEmpty()) {
            if (DEBUG) {
                log.debug("Processing suspects termination [suspects={}]", suspects.values());
            }

            long now = System.nanoTime();

            for (Iterator<Suspect> it = suspects.values().iterator(); it.hasNext(); ) {
                Suspect suspect = it.next();

                if (now - suspect.timestamp() >= maxFailedNodeTimeout) {
                    if (suspect.isDead(liveNodes, quorumSize)) {
                        if (terminated == null) {
                            terminated = new ArrayList<>();
                        }

                        terminated.add(suspect.id());

                        it.remove();

                        if (DEBUG) {
                            log.debug("Suspect terminated [suspect={}]", suspect);
                        }
                    }
                }
            }
        }

        return terminated == null ? Collections.emptyList() : terminated;
    }
}
