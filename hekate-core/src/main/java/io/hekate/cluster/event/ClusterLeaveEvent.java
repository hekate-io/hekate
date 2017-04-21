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

package io.hekate.cluster.event;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import java.util.Set;

/**
 * Cluster leave event.
 *
 * <p>
 * This event is fired by the {@link ClusterService} every time when local node leaves the cluster. For more details about the cluster
 * events processing please see the documentation of {@link ClusterEventListener} interface.
 * </p>
 *
 * @see ClusterEventListener
 */
public class ClusterLeaveEvent extends ClusterEventBase {
    private static final long serialVersionUID = 1;

    private final Set<ClusterNode> added;

    private final Set<ClusterNode> removed;

    /**
     * Constructs a new instance.
     *
     * @param topology Topology.
     * @param added Set of newly joined nodes (see {@link #getAdded()}).
     * @param removed Set of nodes that left the cluster (see {@link #getRemoved()}).
     */
    public ClusterLeaveEvent(ClusterTopology topology, Set<ClusterNode> added, Set<ClusterNode> removed) {
        super(topology);

        this.added = added;
        this.removed = removed;
    }

    /**
     * Returns the set of new nodes that joined the cluster.
     *
     * @return Set of new nodes that joined the cluster.
     */
    public Set<ClusterNode> getAdded() {
        return added;
    }

    /**
     * Returns the set of nodes that left the cluster.
     *
     * @return Set of nodes that left the cluster.
     */
    public Set<ClusterNode> getRemoved() {
        return removed;
    }

    /**
     * Returns {@link ClusterEventType#LEAVE}.
     *
     * @return {@link ClusterEventType#LEAVE}.
     */
    @Override
    public ClusterEventType getType() {
        return ClusterEventType.LEAVE;
    }

    /**
     * Returns this instance.
     *
     * @return This instance.
     */
    @Override
    public ClusterLeaveEvent asLeave() {
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[added=" + getAdded()
            + ", removed=" + getRemoved()
            + ", topology=" + getTopology()
            + ']';
    }
}
