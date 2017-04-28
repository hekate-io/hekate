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

import io.hekate.cluster.ClusterUuid;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GossipDigest extends GossipBase {
    private final long version;

    private final Map<ClusterUuid, GossipNodeInfo> members;

    private final Set<ClusterUuid> removed;

    private final Set<ClusterUuid> seen;

    public GossipDigest(long version, Map<ClusterUuid, GossipNodeInfo> members, Set<ClusterUuid> removed,
        Set<ClusterUuid> seen) {
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

        Map<ClusterUuid, GossipNodeState> members = gossip.getMembers();

        Map<ClusterUuid, GossipNodeInfo> membersInfo;

        if (members.isEmpty()) {
            membersInfo = Collections.emptyMap();
        } else {
            membersInfo = new HashMap<>(members.size(), 1.0f);

            for (Map.Entry<ClusterUuid, GossipNodeState> e : members.entrySet()) {
                ClusterUuid id = e.getKey();
                GossipNodeState node = e.getValue();

                membersInfo.put(id, new GossipNodeInfo(node.getId(), node.getStatus(), node.getVersion()));
            }

            membersInfo = Collections.unmodifiableMap(membersInfo);
        }

        this.version = gossip.getVersion();
        this.members = membersInfo;
        this.removed = gossip.getRemoved();
        this.seen = gossip.getSeen();
    }

    @Override
    public Map<ClusterUuid, GossipNodeInfo> getMembersInfo() {
        return members;
    }

    @Override
    public Set<ClusterUuid> getSeen() {
        return seen;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public Set<ClusterUuid> getRemoved() {
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
