/*
 * Copyright 2020 The Hekate Project
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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.hekate.cluster.internal.gossip.GossipNodeStatus.JOINING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.LEAVING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.UP;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class Gossip extends GossipBase {
    private final long epoch;

    private final Map<ClusterNodeId, GossipNodeState> members;

    private final Set<ClusterNodeId> removed;

    private final Set<ClusterNodeId> seen;

    private final int maxJoinOrder;

    private String toStringCache;

    private Boolean convergenceCache;

    public Gossip() {
        epoch = 0;
        maxJoinOrder = 0;
        members = emptyMap();
        seen = emptySet();
        removed = emptySet();
    }

    public Gossip(
        long epoch,
        Map<ClusterNodeId, GossipNodeState> members,
        Set<ClusterNodeId> removed,
        Set<ClusterNodeId> seen,
        int maxJoinOrder
    ) {
        this.epoch = epoch;
        this.members = members;
        this.removed = removed;
        this.seen = seen;
        this.maxJoinOrder = maxJoinOrder;
    }

    public int maxJoinOrder() {
        return maxJoinOrder;
    }

    public Gossip maxJoinOrder(int maxJoinOrder) {
        if (this.maxJoinOrder == maxJoinOrder) {
            return this;
        }

        return new Gossip(epoch, members, removed, seen, maxJoinOrder);
    }

    public GossipNodeState member(ClusterNodeId id) {
        return members.get(id);
    }

    public Map<ClusterNodeId, GossipNodeState> members() {
        return members;
    }

    public boolean hasMembers() {
        return !members.isEmpty();
    }

    @Override
    public Map<ClusterNodeId, ? extends GossipNodeInfoBase> membersInfo() {
        return members;
    }

    public Stream<GossipNodeState> stream() {
        return members.values().stream();
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public Set<ClusterNodeId> removed() {
        return removed;
    }

    public boolean isDown(ClusterNodeId id) {
        GossipNodeState member = member(id);

        return (member != null && member.status().isTerminated()) || removed.contains(id);
    }

    public Gossip merge(ClusterNodeId localNodeId, Gossip other) {
        Map<ClusterNodeId, GossipNodeState> thisMembers = members();
        Map<ClusterNodeId, GossipNodeState> otherMembers = other.members();

        Set<ClusterNodeId> removed = epoch() > other.epoch() ? removed() : other.removed();

        // Members.
        Map<ClusterNodeId, GossipNodeState> newMembers = new HashMap<>();

        thisMembers.forEach((id, member) -> {
            GossipNodeState otherMember = otherMembers.get(id);

            if (otherMember == null) {
                if (!removed.contains(id)) {
                    newMembers.put(id, member);
                }
            } else {
                newMembers.put(id, member.merge(otherMember));
            }
        });

        otherMembers.entrySet().stream()
            .filter(e -> !newMembers.containsKey(e.getKey()))
            .forEach(e -> {
                ClusterNodeId id = e.getKey();

                if (!removed.contains(id)) {
                    newMembers.put(e.getKey(), e.getValue());
                }
            });

        // Suspects.
        GossipNodeState localState = newMembers.get(localNodeId);

        if (localState != null && localState.hasSuspected()) {
            Set<ClusterNodeId> unsuspected = null;

            for (ClusterNodeId suspected : localState.suspected()) {
                if (!newMembers.containsKey(suspected)) {
                    if (unsuspected == null) {
                        unsuspected = new HashSet<>();
                    }

                    unsuspected.add(suspected);
                }
            }

            if (unsuspected != null) {
                newMembers.put(localNodeId, localState.unsuspect(unsuspected));
            }
        }

        // Epoch.
        long newEpoch = Math.max(epoch(), other.epoch());

        // Seen.
        Set<ClusterNodeId> newSeen = new HashSet<>();

        newSeen.add(localNodeId);

        thisMembers.values().forEach(n -> {
            if (n.status().isTerminated() && hasSeen(n.id()) && newMembers.containsKey(n.id())) {
                newSeen.add(n.id());
            }
        });

        otherMembers.values().forEach(n -> {
            if (n.status().isTerminated() && other.hasSeen(n.id()) && newMembers.containsKey(n.id())) {
                newSeen.add(n.id());
            }
        });

        // Max join order.
        int newMaxJoinOrder = Integer.max(maxJoinOrder(), other.maxJoinOrder());

        return new Gossip(newEpoch, unmodifiableMap(newMembers), removed, unmodifiableSet(newSeen), newMaxJoinOrder);
    }

    public Gossip update(ClusterNodeId localNodeId, GossipNodeState modified) {
        return update(localNodeId, singletonList(modified));
    }

    public Gossip update(ClusterNodeId localNodeId, List<GossipNodeState> modified) {
        Set<ClusterNodeId> newSeen = new HashSet<>();

        newSeen.add(localNodeId);

        members.values().forEach(n -> {
            if (n.status().isTerminated() && hasSeen(n.id())) {
                newSeen.add(n.id());
            }
        });

        Map<ClusterNodeId, GossipNodeState> newMembers = new HashMap<>(members);

        modified.forEach(n ->
            newMembers.put(n.id(), n)
        );

        return new Gossip(epoch, unmodifiableMap(newMembers), removed, unmodifiableSet(newSeen), maxJoinOrder);
    }

    public Gossip purge(ClusterNodeId localNodeId, Set<ClusterNodeId> removed) {
        Set<ClusterNodeId> newSeen = new HashSet<>();

        newSeen.add(localNodeId);

        members.values().forEach(m -> {
            if (m.status().isTerminated() && !removed.contains(m.id()) && hasSeen(m.id())) {
                newSeen.add(m.id());
            }
        });

        Map<ClusterNodeId, GossipNodeState> newMembers = new HashMap<>(members);

        GossipNodeState localNodeState = newMembers.get(localNodeId);

        localNodeState = localNodeState.unsuspect(removed);

        newMembers.put(localNodeId, localNodeState);

        newMembers.keySet().removeAll(removed);

        Set<ClusterNodeId> newRemoved = new HashSet<>(removed);

        newRemoved = unmodifiableSet(newRemoved);
        newMembers = unmodifiableMap(newMembers);

        return new Gossip(epoch + 1, newMembers, newRemoved, newSeen, maxJoinOrder);
    }

    @Override
    public Set<ClusterNodeId> seen() {
        return seen;
    }

    public Gossip seen(ClusterNodeId localNodeId) {
        if (seen.contains(localNodeId)) {
            return this;
        } else {
            Set<ClusterNodeId> newSeen = new HashSet<>(seen);

            newSeen.add(localNodeId);

            return new Gossip(epoch, members, removed, unmodifiableSet(newSeen), maxJoinOrder);
        }
    }

    public Gossip seen(Collection<ClusterNodeId> newSeen) {
        if (seen.containsAll(newSeen)) {
            return this;
        } else {
            Set<ClusterNodeId> mergedSeen = new HashSet<>(seen);

            mergedSeen.addAll(newSeen);

            return new Gossip(epoch, members, removed, unmodifiableSet(mergedSeen), maxJoinOrder);
        }
    }

    public Gossip inheritSeen(ClusterNodeId localNodeId, GossipBase other) {
        Set<ClusterNodeId> newSeen = null;

        for (GossipNodeInfoBase n : other.membersInfo().values()) {
            ClusterNodeId otherId = n.id();

            if (n.status().isTerminated() && hasMember(otherId) && !hasSeen(otherId) && other.hasSeen(otherId)) {
                if (newSeen == null) {
                    newSeen = new HashSet<>(seen);
                }

                newSeen.add(otherId);
            }
        }

        if (newSeen == null && hasSeen(localNodeId)) {
            return this;
        }

        if (newSeen == null) {
            newSeen = new HashSet<>(seen);
        }

        newSeen.add(localNodeId);

        return new Gossip(epoch, members, removed, unmodifiableSet(newSeen), maxJoinOrder);
    }

    public boolean isConvergent() {
        Boolean convergent = convergenceCache;

        if (convergent == null) {
            convergent = true;

            Set<ClusterNodeId> allSuspected = null;

            for (GossipNodeState node : members.values()) {
                if (!seen.contains(node.id())) {
                    if (node.status().isTerminated()) {
                        if (allSuspected == null) {
                            allSuspected = allSuspected();
                        }

                        if (!allSuspected.contains(node.id())) {
                            convergent = false;

                            break;
                        }
                    } else {
                        convergent = false;

                        break;
                    }
                }
            }

            convergenceCache = convergent;
        }

        return convergent;
    }

    public Set<ClusterNodeId> allSuspected() {
        Set<ClusterNodeId> allSuspected = new HashSet<>();

        members.values().forEach(m ->
            allSuspected.addAll(m.suspected())
        );

        return allSuspected;
    }

    public boolean isCoordinator(ClusterNodeId localNode) {
        ClusterNode coordinator = coordinator(localNode);

        return coordinator != null && coordinator.id().equals(localNode);
    }

    public ClusterNode coordinator(ClusterNodeId localNode) {
        Set<ClusterNodeId> allSuspected = allSuspected();

        // Ignore false suspicions of the local node.
        allSuspected.remove(localNode);

        return members.values().stream()
            .filter(n -> isCoordinatorStatus(n) && !allSuspected.contains(n.id()))
            .map(GossipNodeState::node)
            .sorted()
            .findFirst().orElse(null);
    }

    public GossipSuspectView suspectView() {
        Map<ClusterNodeId, Set<ClusterNodeId>> suspected = null;

        for (GossipNodeState n : members.values()) {
            if (!n.suspected().isEmpty()) {
                if (suspected == null) {
                    suspected = new HashMap<>();
                }

                for (ClusterNodeId id : n.suspected()) {
                    Set<ClusterNodeId> suspectedBy = suspected.computeIfAbsent(id, k -> new HashSet<>());

                    suspectedBy.add(n.id());
                }
            }
        }

        if (suspected != null) {
            suspected.replaceAll((id, set) -> unmodifiableSet(set));

            return new GossipSuspectView(unmodifiableMap(suspected));
        } else {
            return GossipSuspectView.EMPTY;
        }
    }

    public boolean isSuspected(ClusterNodeId id) {
        return members.values().stream().anyMatch(n -> n.isSuspected(id));
    }

    private boolean isCoordinatorStatus(GossipNodeState state) {
        GossipNodeStatus status = state.status();

        return status == JOINING
            || status == UP
            || status == LEAVING;
    }

    @Override
    public String toString() {
        String str = toStringCache;

        if (str == null) {
            toStringCache = str = getClass().getSimpleName()
                + "[members-size=" + members.size()
                + ", seen-size=" + seen.size()
                + ", epoch=" + epoch
                + ", max-order=" + maxJoinOrder
                + ", members=" + members.values()
                + ", seen=" + seen
                + ", removed=" + removed
                + ']';
        }

        return str;
    }
}
