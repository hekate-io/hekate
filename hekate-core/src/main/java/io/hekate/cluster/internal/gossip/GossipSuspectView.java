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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GossipSuspectView {
    public static final GossipSuspectView EMPTY = new GossipSuspectView(Collections.emptyMap());

    private final Map<ClusterNodeId, Set<ClusterNodeId>> suspected;

    public GossipSuspectView(Map<ClusterNodeId, Set<ClusterNodeId>> suspected) {
        assert suspected != null : "Suspect map is null.";

        this.suspected = suspected;
    }

    public Set<ClusterNodeId> suspecting(ClusterNodeId id) {
        return suspected.get(id);
    }

    public boolean isEmpty() {
        return suspected.isEmpty();
    }

    public Set<ClusterNodeId> suspected() {
        return suspected.keySet();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
