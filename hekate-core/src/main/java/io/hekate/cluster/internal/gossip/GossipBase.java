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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.hekate.cluster.internal.gossip.GossipPrecedence.AFTER;
import static io.hekate.cluster.internal.gossip.GossipPrecedence.BEFORE;
import static io.hekate.cluster.internal.gossip.GossipPrecedence.CONCURRENT;
import static io.hekate.cluster.internal.gossip.GossipPrecedence.SAME;

public abstract class GossipBase {
    public abstract Map<ClusterNodeId, ? extends GossipNodeInfoBase> membersInfo();

    public abstract Set<ClusterNodeId> seen();

    public abstract long version();

    public abstract Set<ClusterNodeId> removed();

    public boolean hasSeen(ClusterNodeId id) {
        assert id != null : "Node id is null.";

        return seen().contains(id);
    }

    public boolean hasSeenAll(Set<ClusterNodeId> ids) {
        assert ids != null : "Nodes set is null.";

        return seen().containsAll(ids);
    }

    public boolean hasMember(ClusterNodeId id) {
        return membersInfo().containsKey(id);
    }

    public GossipPrecedence compare(GossipBase other) {
        assert other != null : "Other gossip digest is null.";

        return compareVersions(this, other);
    }

    private static GossipPrecedence compareVersions(GossipBase g1, GossipBase g2) {
        Map<ClusterNodeId, ? extends GossipNodeInfoBase> members1 = g1.membersInfo();
        Map<ClusterNodeId, ? extends GossipNodeInfoBase> members2 = g2.membersInfo();

        Set<ClusterNodeId> allIds = new HashSet<>();

        allIds.addAll(members1.keySet());
        allIds.addAll(members2.keySet());

        long ver1 = g1.version();
        long ver2 = g2.version();

        Set<ClusterNodeId> removed;

        GossipPrecedence precedence;

        if (ver1 == ver2) {
            precedence = SAME;

            removed = g1.removed();
        } else if (ver1 > ver2) {
            precedence = AFTER;

            removed = g1.removed();
        } else {
            precedence = BEFORE;

            removed = g2.removed();
        }

        for (ClusterNodeId id : allIds) {
            GossipNodeInfoBase n1 = members1.get(id);
            GossipNodeInfoBase n2 = members2.get(id);

            if (n1 == null) {
                if (removed.contains(id)) {
                    if (precedence == SAME || precedence == AFTER) {
                        precedence = AFTER;
                    } else {
                        precedence = CONCURRENT;
                    }
                } else {
                    if (precedence == SAME || precedence == BEFORE) {
                        precedence = BEFORE;
                    } else {
                        precedence = CONCURRENT;
                    }
                }
            } else if (n2 == null) {
                if (removed.contains(id)) {
                    if (precedence == SAME || precedence == BEFORE) {
                        precedence = BEFORE;
                    } else {
                        precedence = CONCURRENT;
                    }
                } else {
                    if (precedence == SAME || precedence == AFTER) {
                        precedence = AFTER;
                    } else {
                        precedence = CONCURRENT;
                    }
                }
            } else {
                GossipPrecedence nodePrecedence = n1.compare(n2);

                if (nodePrecedence != SAME) {
                    if ((precedence == BEFORE && nodePrecedence == AFTER) || (precedence == AFTER && nodePrecedence == BEFORE)) {
                        precedence = CONCURRENT;
                    } else {
                        precedence = nodePrecedence;
                    }
                }
            }

            if (precedence == CONCURRENT) {
                break;
            }
        }

        return precedence;
    }
}
