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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterUuid;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

public class GossipNodeState extends GossipNodeInfoBase implements Comparable<GossipNodeState> {
    private static final long serialVersionUID = 1;

    private final ClusterNode node;

    private final GossipNodeStatus status;

    private final long version;

    private final Set<ClusterUuid> suspected;

    public GossipNodeState(ClusterNode node, GossipNodeStatus status) {
        this(node, status, 1, Collections.emptySet());
    }

    public GossipNodeState(ClusterNode node, GossipNodeStatus status, long version, Set<ClusterUuid> suspected) {
        assert node != null : "Node is null.";
        assert status != null : "Status is null.";
        assert suspected != null : "Suspected set is null.";

        this.node = node;
        this.status = status;
        this.version = version;
        this.suspected = suspected;
    }

    public GossipNodeState merge(GossipNodeState other) {
        if (status != other.status || version != other.version) {
            GossipNodeStatus newStatus;
            long newVersion;
            Set<ClusterUuid> newSuspected;
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

    public GossipNodeState status(GossipNodeStatus newStatus) {
        if (status == newStatus) {
            return this;
        }

        return new GossipNodeState(node, newStatus, version, suspected);
    }

    public GossipNodeState order(int order) {
        if (node.getJoinOrder() == order) {
            return this;
        }

        DefaultClusterNode nodeCopy = new DefaultClusterNode(node);

        nodeCopy.setJoinOrder(order);

        return new GossipNodeState(nodeCopy, status, version, suspected);
    }

    public GossipNodeState suspected(ClusterUuid suspected) {
        return suspected(Collections.singleton(suspected));
    }

    public GossipNodeState suspected(Set<ClusterUuid> suspected) {
        return new GossipNodeState(node, status, version + 1, unmodifiableSet(suspected));
    }

    public GossipNodeState unsuspected(ClusterUuid removed) {
        return unsuspected(Collections.singleton(removed));
    }

    public GossipNodeState unsuspected(Set<ClusterUuid> unsuspected) {
        Set<ClusterUuid> newSuspected = null;

        for (ClusterUuid removedId : unsuspected) {
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

    public Set<ClusterUuid> getSuspected() {
        return suspected;
    }

    public boolean isSuspected(ClusterUuid id) {
        return suspected.contains(id);
    }

    public boolean hasSuspected() {
        return !suspected.isEmpty();
    }

    @Override
    public ClusterUuid getId() {
        return node.getId();
    }

    public int getOrder() {
        return node.getJoinOrder();
    }

    public ClusterNode getNode() {
        return node;
    }

    public ClusterAddress getNodeAddress() {
        return node.getAddress();
    }

    @Override
    public GossipNodeStatus getStatus() {
        return status;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public int compareTo(GossipNodeState o) {
        return getId().compareTo(o.getId());
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

        return getId().equals(that.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
