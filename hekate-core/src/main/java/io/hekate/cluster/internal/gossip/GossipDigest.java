/*
 * Copyright 2022 The Hekate Project
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class GossipDigest extends GossipBase {
    private final long epoch;

    private final Map<ClusterNodeId, GossipNodeInfo> members;

    private final Set<ClusterNodeId> removed;

    private final Set<ClusterNodeId> seen;

    public GossipDigest(
        long epoch,
        Map<ClusterNodeId, GossipNodeInfo> members,
        Set<ClusterNodeId> removed,
        Set<ClusterNodeId> seen
    ) {
        this.epoch = epoch;
        this.members = members;
        this.removed = removed;
        this.seen = seen;
    }

    public GossipDigest(Gossip gossip) {
        Map<ClusterNodeId, GossipNodeState> members = gossip.members();

        Map<ClusterNodeId, GossipNodeInfo> membersInfo;

        if (members.isEmpty()) {
            membersInfo = emptyMap();
        } else {
            membersInfo = new HashMap<>(members.size(), 1.0f);

            for (Map.Entry<ClusterNodeId, GossipNodeState> e : members.entrySet()) {
                ClusterNodeId id = e.getKey();
                GossipNodeState node = e.getValue();

                membersInfo.put(id, new GossipNodeInfo(node.id(), node.status(), node.version()));
            }

            membersInfo = unmodifiableMap(membersInfo);
        }

        this.epoch = gossip.epoch();
        this.members = membersInfo;
        this.removed = gossip.removed();
        this.seen = gossip.seen();
    }

    @Override
    public Map<ClusterNodeId, GossipNodeInfo> membersInfo() {
        return members;
    }

    @Override
    public Set<ClusterNodeId> seen() {
        return seen;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public Set<ClusterNodeId> removed() {
        return removed;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[members-size=" + members.size()
            + ", seen-size=" + seen.size()
            + ", epoch=" + epoch
            + ", members=" + members.values()
            + ", seen=" + seen
            + ", removed=" + removed
            + ']';
    }
}
