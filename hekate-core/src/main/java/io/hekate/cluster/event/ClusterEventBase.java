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

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import java.io.Serializable;

/**
 * Abstract base class for {@link ClusterEvent} implementations.
 *
 * @see ClusterEventListener
 * @see ClusterService
 */
public abstract class ClusterEventBase implements ClusterEvent, Serializable {
    private static final long serialVersionUID = 1;

    private final ClusterTopology topology;

    /**
     * Constructs a new instance with the specified topology snapshot.
     *
     * @param topology Topology.
     */
    public ClusterEventBase(ClusterTopology topology) {
        this.topology = topology;
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    @Override
    public ClusterJoinEvent asJoin() {
        return null;
    }

    @Override
    public ClusterLeaveEvent asLeave() {
        return null;
    }

    @Override
    public ClusterChangeEvent asChange() {
        return null;
    }
}
