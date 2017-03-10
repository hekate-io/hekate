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

import io.hekate.cluster.ClusterNodeId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.hekate.cluster.internal.gossip.ComparisonResult.AFTER;
import static io.hekate.cluster.internal.gossip.ComparisonResult.BEFORE;
import static io.hekate.cluster.internal.gossip.ComparisonResult.CONCURRENT;
import static io.hekate.cluster.internal.gossip.ComparisonResult.SAME;

public abstract class GossipBase {
    public abstract Map<ClusterNodeId, ? extends GossipNodeInfoBase> getMembersInfo();

    public abstract Set<ClusterNodeId> getSeen();

    public abstract long getVersion();

    public abstract Set<ClusterNodeId> getRemoved();

    public boolean hasSeen(ClusterNodeId id) {
        assert id != null : "Node id is null.";

        return getSeen().contains(id);
    }

    public boolean hasSeen(Set<ClusterNodeId> ids) {
        assert ids != null : "Nodes set is null.";

        return getSeen().containsAll(ids);
    }

    public boolean hasMember(ClusterNodeId id) {
        return getMembersInfo().containsKey(id);
    }

    public ComparisonResult compare(GossipBase other) {
        assert other != null : "Other gossip digest is null.";

        return compareVersions(this, other);
    }

    private static ComparisonResult compareVersions(GossipBase g1, GossipBase g2) {
        Map<ClusterNodeId, ? extends GossipNodeInfoBase> members1 = g1.getMembersInfo();
        Map<ClusterNodeId, ? extends GossipNodeInfoBase> members2 = g2.getMembersInfo();

        Set<ClusterNodeId> allIds = new HashSet<>();

        allIds.addAll(members1.keySet());
        allIds.addAll(members2.keySet());

        long ver1 = g1.getVersion();
        long ver2 = g2.getVersion();

        Set<ClusterNodeId> removed;

        ComparisonResult result;

        if (ver1 == ver2) {
            result = SAME;

            removed = g1.getRemoved();
        } else if (ver1 > ver2) {
            result = AFTER;

            removed = g1.getRemoved();
        } else {
            result = BEFORE;

            removed = g2.getRemoved();
        }

        for (ClusterNodeId id : allIds) {
            GossipNodeInfoBase n1 = members1.get(id);
            GossipNodeInfoBase n2 = members2.get(id);

            if (n1 == null) {
                if (removed.contains(id)) {
                    if (result == SAME || result == AFTER) {
                        result = AFTER;
                    } else {
                        result = CONCURRENT;
                    }
                } else {
                    if (result == SAME || result == BEFORE) {
                        result = BEFORE;
                    } else {
                        result = CONCURRENT;
                    }
                }
            } else if (n2 == null) {
                if (removed.contains(id)) {
                    if (result == SAME || result == BEFORE) {
                        result = BEFORE;
                    } else {
                        result = CONCURRENT;
                    }
                } else {
                    if (result == SAME || result == AFTER) {
                        result = AFTER;
                    } else {
                        result = CONCURRENT;
                    }
                }
            } else {
                ComparisonResult nodeResult = n1.compare(n2);

                if (nodeResult != SAME) {
                    if (result == BEFORE && nodeResult == AFTER
                        || result == AFTER && nodeResult == BEFORE) {
                        result = CONCURRENT;
                    } else {
                        result = nodeResult;
                    }
                }
            }

            if (result == CONCURRENT) {
                break;
            }
        }

        return result;
    }
}
