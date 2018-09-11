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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static io.hekate.cluster.internal.gossip.GossipNodeStatus.DOWN;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.JOINING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.LEAVING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.UP;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class Gossip extends GossipBase {
    private final long version;

    private final Map<ClusterNodeId, GossipNodeState> members;

    private final Set<ClusterNodeId> removed;

    private final Set<ClusterNodeId> seen;

    private final int maxJoinOrder;

    private String toStringCache;

    private Boolean convergenceCache;

    public Gossip() {
        version = 0;
        maxJoinOrder = 0;
        members = Collections.emptyMap();
        seen = emptySet();
        removed = emptySet();
    }

    public Gossip(long version, Map<ClusterNodeId, GossipNodeState> members, Set<ClusterNodeId> removed, Set<ClusterNodeId> seen,
        int maxJoinOrder) {
        assert members != null : "Members map is null.";
        assert removed != null : "Removed set is null.";
        assert seen != null : "Seen set is null.";

        this.version = version;
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

        return new Gossip(version, members, removed, seen, maxJoinOrder);
    }

    public GossipNodeState member(ClusterNodeId id) {
        assert id != null : "Node id is null.";

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
    public long version() {
        return version;
    }

    @Override
    public Set<ClusterNodeId> removed() {
        return removed;
    }

    public boolean isDown(ClusterNodeId id) {
        GossipNodeState member = member(id);

        return (member != null && member.status() == DOWN) || removed.contains(id);
    }

    public Gossip merge(ClusterNodeId nodeId, Gossip other) {
        assert nodeId != null : "Node id is null.";
        assert other != null : "Other gossip is null.";

        Map<ClusterNodeId, GossipNodeState> thisMembers = members();
        Map<ClusterNodeId, GossipNodeState> otherMembers = other.members();

        Set<ClusterNodeId> removed = version() > other.version() ? removed() : other.removed();

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

        GossipNodeState state = newMembers.get(nodeId);

        if (state != null && state.hasSuspected()) {
            Set<ClusterNodeId> unsuspected = null;

            for (ClusterNodeId suspected : state.suspected()) {
                if (!newMembers.containsKey(suspected)) {
                    if (unsuspected == null) {
                        unsuspected = new HashSet<>();
                    }

                    unsuspected.add(suspected);
                }
            }

            if (unsuspected != null) {
                newMembers.put(nodeId, state.unsuspected(unsuspected));
            }
        }

        long newVersion = Math.max(version(), other.version());

        Set<ClusterNodeId> newSeen = new HashSet<>();

        thisMembers.values().forEach(m -> {
            if (m.status() == DOWN && hasSeen(m.id()) && newMembers.containsKey(m.id())) {
                newSeen.add(m.id());
            }
        });

        otherMembers.values().forEach(m -> {
            if (m.status() == DOWN && other.hasSeen(m.id()) && newMembers.containsKey(m.id())) {
                newSeen.add(m.id());
            }
        });

        newSeen.add(nodeId);

        int newMaxJoinOrder = Integer.max(maxJoinOrder(), other.maxJoinOrder());

        return new Gossip(newVersion, unmodifiableMap(newMembers), removed, unmodifiableSet(newSeen), newMaxJoinOrder);
    }

    public Gossip update(ClusterNodeId id, GossipNodeState modified) {
        return update(id, Collections.singletonList(modified));
    }

    public Gossip update(ClusterNodeId id, List<GossipNodeState> modified) {
        assert id != null : "Node id is null.";
        assert modified != null : "Modified nodes list is null.";
        assert modified.stream().noneMatch(Objects::isNull) : "Modified nodes list contains null value.";

        Set<ClusterNodeId> newSeen = new HashSet<>();

        members.values().forEach(m -> {
            if (m.status() == DOWN && hasSeen(m.id())) {
                newSeen.add(m.id());
            }
        });

        newSeen.add(id);

        Map<ClusterNodeId, GossipNodeState> newMembers = new HashMap<>(members);

        modified.forEach(s -> newMembers.put(s.id(), s));

        return new Gossip(version, unmodifiableMap(newMembers), removed, unmodifiableSet(newSeen), maxJoinOrder);
    }

    public Gossip purge(ClusterNodeId id, Set<ClusterNodeId> removed) {
        assert id != null : "Node id is null.";
        assert removed != null : "Removed nodes set is null.";
        assert !removed.isEmpty() : "Removed nodes set is empty.";
        assert removed.stream().noneMatch(Objects::isNull) : "Removed nodes set contains null value.";

        Set<ClusterNodeId> newSeen = new HashSet<>();

        members.values().forEach(m -> {
            if (m.status() == DOWN && !removed.contains(m.id()) && hasSeen(m.id())) {
                newSeen.add(m.id());
            }
        });

        newSeen.add(id);

        Map<ClusterNodeId, GossipNodeState> newMembers = new HashMap<>(members);

        GossipNodeState nodeState = newMembers.get(id);

        nodeState = nodeState.unsuspected(removed);

        newMembers.put(id, nodeState);

        newMembers.keySet().removeAll(removed);

        Set<ClusterNodeId> newRemoved = new HashSet<>(removed);

        newRemoved = unmodifiableSet(newRemoved);
        newMembers = unmodifiableMap(newMembers);

        return new Gossip(version + 1, newMembers, newRemoved, newSeen, maxJoinOrder);
    }

    @Override
    public Set<ClusterNodeId> seen() {
        return seen;
    }

    public Gossip seen(ClusterNodeId id) {
        assert hasMember(id) : "Node is not a member [id=" + id + ", members=" + members.values() + ']';

        if (this.seen.contains(id)) {
            return this;
        } else {
            Set<ClusterNodeId> newSeen = new HashSet<>(this.seen);

            newSeen.add(id);

            return new Gossip(version, members, removed, unmodifiableSet(newSeen), maxJoinOrder);
        }
    }

    public Gossip seen(Collection<ClusterNodeId> seen) {
        assert seen != null : "Seen nodes list is null.";
        assert seen.stream().noneMatch(Objects::isNull) : "Seen nodes list contains null value.";
        assert members.keySet().containsAll(seen) : "Seen nodes are not members [seen=" + seen + ", members=" + members.values() + ']';

        if (this.seen.containsAll(seen)) {
            return this;
        } else {
            Set<ClusterNodeId> newSeen = new HashSet<>(this.seen);

            newSeen.addAll(seen);

            return new Gossip(version, members, removed, unmodifiableSet(newSeen), maxJoinOrder);
        }
    }

    public Gossip inheritSeen(ClusterNodeId id, GossipBase other) {
        assert id != null : "Cluster node id is null.";
        assert other != null : "Other gossip is null.";
        assert hasMember(id) : "Seen node is not within members list.";

        Set<ClusterNodeId> newSeen = null;

        for (GossipNodeInfoBase s : other.membersInfo().values()) {
            ClusterNodeId otherId = s.id();

            if (s.status() == DOWN && hasMember(otherId) && !hasSeen(otherId) && other.hasSeen(otherId)) {
                if (newSeen == null) {
                    newSeen = new HashSet<>(seen);
                }

                newSeen.add(otherId);
            }
        }

        if (newSeen == null && hasSeen(id)) {
            return this;
        }

        if (newSeen == null) {
            newSeen = new HashSet<>(seen);
        }

        newSeen.add(id);

        return new Gossip(version, members, removed, unmodifiableSet(newSeen), maxJoinOrder);
    }

    public boolean isConvergent() {
        Boolean convergent = convergenceCache;

        if (convergent == null) {
            convergent = true;

            Set<ClusterNodeId> seen = this.seen;

            Set<ClusterNodeId> allSuspected = null;

            for (GossipNodeState node : members.values()) {
                if (!seen.contains(node.id())) {
                    if (node.status() == DOWN) {
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
        Set<ClusterNodeId> allFailed = new HashSet<>();

        members.values().forEach(m -> allFailed.addAll(m.suspected()));

        return allFailed;
    }

    public boolean isCoordinator(ClusterNodeId localNode) {
        assert localNode != null : "Local node id is null.";

        ClusterNode coordinator = coordinator(localNode);

        return coordinator != null && coordinator.id().equals(localNode);
    }

    public ClusterNode coordinator(ClusterNodeId localNode) {
        Set<ClusterNodeId> allSuspected = allSuspected();

        // Ignore false suspicions of the local node.
        allSuspected.remove(localNode);

        return members.values().stream()
            .filter(m -> isCoordinatorStatus(m) && !allSuspected.contains(m.id()))
            .map(GossipNodeState::node)
            .sorted()
            .findFirst().orElse(null);
    }

    public SuspectedNodesView suspectedView() {
        Map<ClusterNodeId, Set<ClusterNodeId>> suspected = null;

        for (GossipNodeState s : members.values()) {
            Set<ClusterNodeId> nodeSuspected = s.suspected();

            if (!nodeSuspected.isEmpty()) {
                if (suspected == null) {
                    suspected = new HashMap<>();
                }

                for (ClusterNodeId id : nodeSuspected) {
                    Set<ClusterNodeId> suspectedBy = suspected.computeIfAbsent(id, k -> new HashSet<>());

                    suspectedBy.add(s.id());
                }
            }
        }

        if (suspected != null) {
            suspected.replaceAll((id, set) -> unmodifiableSet(set));

            suspected = unmodifiableMap(suspected);

            return new SuspectedNodesView(suspected);
        }

        return SuspectedNodesView.EMPTY;
    }

    public boolean isSuspected(ClusterNodeId id) {
        assert id != null : "Cluster node id is null.";

        for (GossipNodeState n : members.values()) {
            if (n.isSuspected(id)) {
                return true;
            }
        }

        return false;
    }

    private boolean isCoordinatorStatus(GossipNodeState state) {
        assert state != null : "Node state is null.";

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
                + ", ver=" + version
                + ", max-order=" + maxJoinOrder
                + ", members=" + members.values()
                + ", seen=" + seen
                + ", removed=" + removed
                + ']';
        }

        return str;
    }
}
