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

package io.hekate.cluster.event;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.HekateSupport;

/**
 * Cluster join event.
 *
 * <p>
 * This event is fired by the {@link ClusterService} every time when local node joins the cluster. For more details about the cluster events
 * processing please see the documentation of {@link ClusterEventListener} interface.
 * </p>
 *
 * @see ClusterEventListener
 */
public class ClusterJoinEvent extends ClusterEventBase {
    /**
     * Constructs a new instance.
     *
     * @param topology Topology.
     * @param hekate Delegate for {@link #hekate()}.
     */
    public ClusterJoinEvent(ClusterTopology topology, HekateSupport hekate) {
        super(topology, hekate);
    }

    /**
     * Returns {@link ClusterEventType#JOIN}.
     *
     * @return {@link ClusterEventType#JOIN}.
     */
    @Override
    public ClusterEventType type() {
        return ClusterEventType.JOIN;
    }

    /**
     * Returns this instance.
     *
     * @return This instance.
     */
    @Override
    public ClusterJoinEvent asJoin() {
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "["
            + "topology=" + topology()
            + ']';
    }
}
