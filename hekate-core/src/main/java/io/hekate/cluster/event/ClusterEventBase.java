/*
 * Copyright 2021 The Hekate Project
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
import io.hekate.core.internal.util.ArgAssert;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract base class for {@link ClusterEvent} implementations.
 *
 * @see ClusterEventListener
 * @see ClusterService
 */
public abstract class ClusterEventBase implements ClusterEvent {
    private static final CompletableFuture[] EMPTY_FUTURES = new CompletableFuture[0];

    private final HekateSupport hekate;

    private final ClusterTopology topology;

    private final List<CompletableFuture<?>> syncs = new CopyOnWriteArrayList<>();

    /**
     * Constructs a new instance with the specified topology snapshot.
     *
     * @param topology Topology.
     * @param hekate Delegate for {@link #hekate()}.
     */
    public ClusterEventBase(ClusterTopology topology, HekateSupport hekate) {
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
    public void attach(CompletableFuture<?> future) {
        ArgAssert.notNull(future, "Future");

        syncs.add(future);
    }

    @Override
    public CompletableFuture<?> future() {
        return CompletableFuture.allOf(syncs.toArray(EMPTY_FUTURES));
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
