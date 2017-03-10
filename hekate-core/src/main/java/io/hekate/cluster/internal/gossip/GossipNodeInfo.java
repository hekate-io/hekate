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

public class GossipNodeInfo extends GossipNodeInfoBase {
    private static final long serialVersionUID = 1;

    private final ClusterNodeId id;

    private final GossipNodeStatus status;

    private final long version;

    private volatile String toStringCache;

    public GossipNodeInfo(ClusterNodeId id, GossipNodeStatus status, long version) {
        this.id = id;
        this.status = status;
        this.version = version;
    }

    @Override
    public ClusterNodeId getId() {
        return id;
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
    public String toString() {
        String str = toStringCache;

        if (str == null) {
            toStringCache = str = getClass().getSimpleName()
                + "[id=" + id
                + ", status=" + status
                + ", version=" + version
                + ']';
        }

        return str;
    }
}
