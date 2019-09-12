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

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

public class GossipNodeState extends GossipNodeInfoBase implements Comparable<GossipNodeState> {
    private final ClusterNode node;

    private final GossipNodeStatus status;

    private final long version;

    private final Set<ClusterNodeId> suspected;

    public GossipNodeState(ClusterNode node, GossipNodeStatus status) {
        this(node, status, 1, Collections.emptySet());
    }

    public GossipNodeState(ClusterNode node, GossipNodeStatus status, long version, Set<ClusterNodeId> suspected) {
        assert node != null : "Node is null.";
        assert status != null : "Status is null.";
        assert suspected != null : "Suspected set is null.";

        this.node = node;
        this.status = status;
        this.version = version;
        this.suspected = suspected;
    }

    @Override
    public ClusterNodeId id() {
        return node.id();
    }

    @Override
    public long version() {
        return version;
    }

    public ClusterNode node() {
        return node;
    }

    public ClusterAddress address() {
        return node.address();
    }

    public GossipNodeState merge(GossipNodeState other) {
        if (status != other.status || version != other.version) {
            GossipNodeStatus newStatus;
            long newVersion;
            Set<ClusterNodeId> newSuspected;
            ClusterNode newNode;

            if (status.compareTo(other.status) >= 0) {
                newStatus = status;
                newNode = node;
            } else {
                newStatus = other.status;
                newNode = other.node;
            }

            if (version >= other.version) {
                newVersion = version;
                newSuspected = suspected;
            } else {
                newVersion = other.version;
                newSuspected = other.suspected;
            }

            return new GossipNodeState(newNode, newStatus, newVersion, newSuspected);
        } else {
            return this;
        }
    }

    @Override
    public GossipNodeStatus status() {
        return status;
    }

    public GossipNodeState status(GossipNodeStatus newStatus) {
        if (status == newStatus) {
            return this;
        }

        return new GossipNodeState(node, newStatus, version, suspected);
    }

    public int order() {
        return node.joinOrder();
    }

    public GossipNodeState order(int order) {
        if (node.joinOrder() == order) {
            return this;
        }

        DefaultClusterNode nodeCopy = new DefaultClusterNode(node);

        nodeCopy.setJoinOrder(order);

        return new GossipNodeState(nodeCopy, status, version, suspected);
    }

    public Set<ClusterNodeId> suspected() {
        return suspected;
    }

    public GossipNodeState suspect(ClusterNodeId suspected) {
        return suspect(Collections.singleton(suspected));
    }

    public GossipNodeState suspect(Set<ClusterNodeId> suspected) {
        return new GossipNodeState(node, status, version + 1, unmodifiableSet(suspected));
    }

    public GossipNodeState unsuspect(ClusterNodeId removed) {
        return unsuspect(Collections.singleton(removed));
    }

    public GossipNodeState unsuspect(Set<ClusterNodeId> unsuspected) {
        Set<ClusterNodeId> newSuspected = null;

        for (ClusterNodeId removedId : unsuspected) {
            if (suspected.contains(removedId)) {
                newSuspected = suspected.stream().filter(id -> !unsuspected.contains(id)).collect(toSet());

                break;
            }
        }

        if (newSuspected == null) {
            return this;
        }

        return new GossipNodeState(node, status, version + 1, unmodifiableSet(newSuspected));
    }

    public boolean isSuspected(ClusterNodeId id) {
        return suspected.contains(id);
    }

    public boolean hasSuspected() {
        return !suspected.isEmpty();
    }

    @Override
    public int compareTo(GossipNodeState o) {
        return id().compareTo(o.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof GossipNodeState)) {
            return false;
        }

        GossipNodeState that = (GossipNodeState)o;

        return id().equals(that.id());
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
