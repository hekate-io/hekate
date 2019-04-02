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
import io.hekate.core.Hekate;
import io.hekate.core.HekateSupport;

/**
 * Abstract base class for {@link ClusterEvent} implementations.
 *
 * @see ClusterEventListener
 * @see ClusterService
 */
public abstract class ClusterEventBase implements ClusterEvent {
    private final HekateSupport hekate;

    private final ClusterTopology topology;

    /**
     * Constructs a new instance with the specified topology snapshot.
     *
     * @param topology Topology.
     * @param hekate Delegate for {@link #hekate()}.
     */
    public ClusterEventBase(ClusterTopology topology, HekateSupport hekate) {
        assert topology != null : "Cluster topology  is null.";
        assert hekate != null : "Hekate is null.";

        this.topology = topology;
        this.hekate = hekate;
    }

    @Override
    public Hekate hekate() {
        return hekate.hekate();
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
