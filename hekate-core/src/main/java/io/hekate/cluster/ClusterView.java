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

import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Filtered view of {@link ClusterService}.
 *
 * <p>
 * Filtering can be done by calling {@link ClusterView#filter(ClusterNodeFilter)} method on the {@link ClusterView} interface. The
 * resulting view will contain only those nodes that match the specified {@link ClusterNodeFilter}. Filtering is also applied to all {@link
 * ClusterEvent}s that are received by {@link ClusterEventListener}s registered to this view.
 * </p>
 *
 * <p>
 * Cluster views can be stacked. If you call {@link #filter(ClusterNodeFilter)} method on a view that is already filtered then the
 * resulting view will contain only those nodes that match both filters (i.e. existing filter of this view and the new filter).
 * </p>
 */
public interface ClusterView extends ClusterFilterSupport<ClusterView>, ClusterTopologySupport {
    /**
     * Registers the cluster event listener.
     *
     * <p>
     * Please see {@link ClusterEventListener} javadocs for more details about cluster events handling.
     * </p>
     *
     * @param listener Listener.
     */
    void addListener(ClusterEventListener listener);

    /**
     * Registers the cluster event listener that will be notified only on events of the specified event types.
     *
     * <p>
     * Please see {@link ClusterEventListener} javadocs for more details about cluster events handling.
     * </p>
     *
     * @param listener Listener.
     * @param eventTypes Types of cluster events that this listener should be notified on.
     */
    void addListener(ClusterEventListener listener, ClusterEventType... eventTypes);

    /**
     * Unregisters the cluster event listener.
     *
     * @param listener Listener.
     */
    void removeListener(ClusterEventListener listener);

    /**
     * Returns a future object that will be completed once the cluster topology matches the specified predicate. If this cluster node
     * leaves the cluster before the specified condition is met then the future object will be {@link CompletableFuture#cancel(boolean)
     * cancelled}.
     *
     * <p>
     * <b>Important:</b> Future object can be completed on the same thread that performs cluster events processing and hence all of its
     * associated {@link CompletionStage}s will be notified on the same thread. If completion stage contains some long running computations
     * then please consider moving them to an asynchronous completion stage
     * (f.e. {@link CompletableFuture#whenCompleteAsync(BiConsumer, Executor)}).
     * </p>
     *
     * @param predicate Topology predicate.
     *
     * @return Topology future.
     */
    CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate);

    /**
     * Awaits for this cluster view to match the specified predicate.
     *
     * <p>
     * This method blocks unless the specified predicate accepts the cluster topology of this view or one of the following happens:
     * </p>
     * <ul>
     * <li>Caller thread gets interrupted</li>
     * <li>This cluster node is stopped</li>
     * </ul>
     *
     * @param predicate Predicate that gets checked every time when underlying cluster topology is changed.
     *
     * @return {@code} if predicate matched with this cluster view; {@code false} in all other cases (thread interruption, node stop, etc).
     */
    boolean awaitFor(Predicate<ClusterTopology> predicate);

    /**
     * Awaits for this cluster view to match the specified predicate up to the specified timeout.
     *
     * <p>
     * This method blocks unless the specified predicate accepts the cluster topology of this view or one of the following happens:
     * </p>
     * <ul>
     * <li>Caller thread gets interrupted</li>
     * <li>This cluster node is stopped</li>
     * <li>Waiting times out</li>
     * </ul>
     *
     * @param predicate Predicate that gets checked every time when underlying cluster topology is changed.
     * @param timeout Timeout.
     * @param timeUnit Time unit.
     *
     * @return {@code} if predicate matched with this cluster view; {@code false} in all other cases (thread interruption, node stop, etc).
     */
    boolean awaitFor(Predicate<ClusterTopology> predicate, long timeout, TimeUnit timeUnit);

    /**
     * Awaits for this cluster view to contain at least one node.
     *
     * <p>
     * This method blocks unless the the cluster topology to have at least one node or one of the following happens:
     * </p>
     * <ul>
     * <li>Caller thread gets interrupted</li>
     * <li>This cluster node is stopped</li>
     * <li>Waiting times out</li>
     * </ul>
     *
     * @return {@code} if this cluster view has at least one node; {@code false} in all other cases (thread interruption, node stop, etc).
     */
    default boolean awaitForNodes() {
        return awaitFor(topology -> !topology.isEmpty());
    }

    /**
     * Awaits for this cluster view to contain at least one node.
     *
     * <p>
     * This method blocks unless the the cluster topology to have at least one node or one of the following happens:
     * </p>
     * <ul>
     * <li>Caller thread gets interrupted</li>
     * <li>This cluster node is stopped</li>
     * <li>Waiting times out</li>
     * </ul>
     *
     * @param timeout Timeout.
     * @param timeUnit Time unit.
     *
     * @return {@code} if predicate matched with this cluster view; {@code false} in all other cases (thread interruption, node stop, etc).
     */
    default boolean awaitForNodes(long timeout, TimeUnit timeUnit) {
        return awaitFor(topology -> !topology.isEmpty(), timeout, timeUnit);
    }

    /**
     * Performs the given action for each node of this view.
     *
     * @param consumer The action to be performed for each node.
     */
    default void forEach(Consumer<ClusterNode> consumer) {
        topology().forEach(consumer);
    }
}
