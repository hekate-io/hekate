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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GossipDigest extends GossipBase {
    private final long version;

    private final Map<ClusterNodeId, GossipNodeInfo> members;

    private final Set<ClusterNodeId> removed;

    private final Set<ClusterNodeId> seen;

    public GossipDigest(long version, Map<ClusterNodeId, GossipNodeInfo> members, Set<ClusterNodeId> removed,
        Set<ClusterNodeId> seen) {
        assert members != null : "Members map is null.";
        assert removed != null : "Removed set is null.";
        assert seen != null : "Seen set is null.";

        this.version = version;
        this.members = members;
        this.removed = removed;
        this.seen = seen;
    }

    public GossipDigest(Gossip gossip) {
        assert gossip != null : "Gossip is null.";

        Map<ClusterNodeId, GossipNodeState> members = gossip.members();

        Map<ClusterNodeId, GossipNodeInfo> membersInfo;

        if (members.isEmpty()) {
            membersInfo = Collections.emptyMap();
        } else {
            membersInfo = new HashMap<>(members.size(), 1.0f);

            for (Map.Entry<ClusterNodeId, GossipNodeState> e : members.entrySet()) {
                ClusterNodeId id = e.getKey();
                GossipNodeState node = e.getValue();

                membersInfo.put(id, new GossipNodeInfo(node.id(), node.status(), node.version()));
            }

            membersInfo = Collections.unmodifiableMap(membersInfo);
        }

        this.version = gossip.version();
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
    public long version() {
        return version;
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
            + ", ver=" + version
            + ", members=" + members.values()
            + ", seen=" + seen
            + ", removed=" + removed
            + ']';
    }
}
