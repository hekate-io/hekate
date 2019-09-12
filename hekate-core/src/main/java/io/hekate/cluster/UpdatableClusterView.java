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

package io.hekate.cluster;

import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.internal.FilteredClusterView;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Simple updatable version of the {@link ClusterView} interface.
 *
 * <p>
 * This class represents an updatable version of the {@link ClusterView} interface and provides support for manual management of this view's
 * cluster topology via {@link #update(ClusterTopology)} method. This class can be used for manual testing of cluster-dependent components.
 * </p>
 *
 * <p>
 * <b>Notice:</b>
 * This class doesn't support neither {@link ClusterEventListener}s nor cluster {@link #futureOf(Predicate) futures}.
 * All methods related to this functionality throw {@link UnsupportedOperationException}s.
 * </p>
 */
public class UpdatableClusterView implements ClusterView {
    /** Updater for {@link #topology} field. */
    private static final AtomicReferenceFieldUpdater<UpdatableClusterView, ClusterTopology> TOPOLOGY = newUpdater(
        UpdatableClusterView.class,
        ClusterTopology.class,
        "topology"
    );

    /** Cluster topology of this view. */
    private volatile ClusterTopology topology;

    /**
     * Constructs a new instance.
     *
     * @param topology Topology.
     */
    protected UpdatableClusterView(ClusterTopology topology) {
        ArgAssert.notNull(topology, "Topology");

        this.topology = topology;
    }

    /**
     * Constructs a new view.
     *
     * @param topology Topology.
     *
     * @return New cluster view.
     */
    public static UpdatableClusterView of(ClusterTopology topology) {
        return new UpdatableClusterView(topology);
    }

    /**
     * Constructs a new view.
     *
     * @param version Topology version (see {@link ClusterTopology#version()}).
     * @param node Cluster node.
     *
     * @return New cluster view.
     */
    public static UpdatableClusterView of(int version, ClusterNode node) {
        ArgAssert.notNull(node, "Node");

        return new UpdatableClusterView(ClusterTopology.of(version, Collections.singleton(node)));
    }

    /**
     * Constructs a new view.
     *
     * @param version Topology version (see {@link ClusterTopology#version()}).
     * @param nodes Cluster nodes.
     *
     * @return New cluster view.
     */
    public static UpdatableClusterView of(int version, Set<ClusterNode> nodes) {
        ArgAssert.notNull(nodes, "Node collection");

        return new UpdatableClusterView(ClusterTopology.of(version, nodes));
    }

    /**
     * Constructs a new empty cluster view.
     *
     * @return New cluster view.
     *
     * @see ClusterTopology#empty()
     */
    public static UpdatableClusterView empty() {
        return new UpdatableClusterView(ClusterTopology.empty());
    }

    /**
     * Updates the cluster topology of this view if the specified topology has higher {@link ClusterTopology#version() version} than the one
     * that is already assigned to this view.
     *
     * @param topology New topology.
     */
    public void update(ClusterTopology topology) {
        ArgAssert.notNull(topology, "Topology");

        TOPOLOGY.accumulateAndGet(this, topology, (oldTopology, newTopology) ->
            newTopology.version() > oldTopology.version() ? newTopology : oldTopology
        );
    }

    @Override
    public ClusterView filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        return new FilteredClusterView(this, filter);
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void addListener(ClusterEventListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void removeListener(ClusterEventListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate, long timeout, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
