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

import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.core.internal.util.ArgAssert;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Immutable snapshot of the cluster topology.
 *
 * <p>
 * Nodes within the cluster topology are sorted based on the comparison rule of {@link ClusterNode#compareTo(ClusterNode)} method.
 * This rule applies to the following methods:
 * </p>
 * <ul>
 * <li>{@link #nodes()}</li>
 * <li>{@link #stream()}</li>
 * <li>{@link #forEach(Consumer)}</li>
 * <li>{@link #iterator()}</li>
 * <li>{@link #spliterator()}</li>
 * <li>{@link #remoteNodes()}</li>
 * <li>{@link #first()}</li>
 * <li>{@link #last()}</li>
 * </ul>
 *
 * @see ClusterService#topology()
 */
public interface ClusterTopology extends Iterable<ClusterNode>, ClusterTopologySupport {
    /**
     * Returns the version of this topology.
     *
     * <p>
     * This method provides support for tacking changes of the cluster topology. The value of this property is
     * monotonically incremented each time when the cluster topology changes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Version is local to each node and can differ from node to node.
     * </p>
     *
     * @return Version this topology.
     */
    long version();

    /**
     * Returns the SHA-256 hash of all {@link ClusterNode#id() cluster node identifiers} of this topology.
     *
     * <p>
     * If this topology is {@link #filter(ClusterNodeFilter) filtered} then only those nodes that match the filter criteria will be used to
     * compute the hash.
     * </p>
     *
     * @return SHA-256 hash of this topology.
     */
    ClusterHash hash();

    /**
     * Returns local node or {@code null} if local node is not within this topology.
     *
     * @return Local node or {@code null} if local node is not within this topology.
     *
     * @see ClusterNode#isLocal()
     */
    ClusterNode localNode();

    /**
     * Returns an immutable list of all nodes with consistent ordering based on {@link ClusterNode#compareTo(ClusterNode)} method.
     * Returns an empty list if there are no nodes within this topology.
     *
     * @return Immutable list of all nodes with consistent ordering or an empty list if there are no nodes within this topology.
     */
    List<ClusterNode> nodes();

    /**
     * Returns the first node of the {@link #nodes()} list or {@code null} if this topology is empty.
     *
     * @return The first node of the {@link #nodes()} list or {@code null} if this topology is empty.
     */
    ClusterNode first();

    /**
     * Returns the last node of the {@link #nodes()} list or {@code null} if this topology is empty.
     *
     * @return The last node of the {@link #nodes()} list or {@code null} if this topology is empty.
     */
    ClusterNode last();

    /**
     * Returns an immutable set of all nodes within this topology. Returns an empty set if there are no nodes within this topology.
     *
     * @return Immutable set of all nodes within this topology or an empty list if there are no nodes within this topology.
     */
    Set<ClusterNode> nodeSet();

    /**
     * Returns an immutable list of remote nodes within this topology ordered by their {@link ClusterNode#id() identifiers}. Returns an
     * empty list if there are no remote nodes within this topology.
     *
     * @return Immutable list of remotes node within this topology ordered by their {@link ClusterNode#id() identifiers} or an empty list
     * if there are no remote nodes within this topology..
     *
     * @see ClusterNode#isLocal()
     */
    List<ClusterNode> remoteNodes();

    /**
     * Returns an immutable set of all nodes ordered by their {@link ClusterNode#joinOrder() join order}. The first element in this set
     * is the oldest node. Returns an empty set if there are no nodes within this topology.
     *
     * @return Immutable set of all nodes ordered by their {@link ClusterNode#joinOrder() join order} or an empty set if there are no
     * nodes within this topology.
     */
    NavigableSet<ClusterNode> joinOrder();

    /**
     * Returns {@link Stream} of all nodes from this topology.
     *
     * @return {@link Stream} of all nodes from this topology.
     */
    Stream<ClusterNode> stream();

    /**
     * Returns {@code true} if this topology contains the specified node.
     *
     * @param node Node to check.
     *
     * @return {@code true} if this topology contains the specified node.
     */
    boolean contains(ClusterNode node);

    /**
     * Returns {@code true} if this topology contains a node with the specified identifier.
     *
     * @param id Node identifier to check.
     *
     * @return {@code true} if this topology contains a node with the specified identifier.
     */
    boolean contains(ClusterNodeId id);

    /**
     * Returns the node for the specified identifier or {@code null} if there is no such node within this topology.
     *
     * @param id Node identifier.
     *
     * @return Node for the specified identifier or {@code null} if there is no such node within this topology.
     */
    ClusterNode get(ClusterNodeId id);

    /**
     * Returns the number of nodes in this topology.
     *
     * @return Number of nodes in this topology.
     */
    int size();

    /**
     * Returns {@code true} if this topology doesn't have any nodes.
     *
     * @return {@code true} if this topology doesn't have any nodes.
     */
    boolean isEmpty();

    /**
     * Returns the oldest node of this topology. Returns {@code null} if this topology is empty.
     *
     * <p>
     * The oldest node is the node with the lowest {@link ClusterNode#joinOrder() join order}.
     * </p>
     *
     * @return Oldest node of this topology or {@code null} if this topology is empty.
     */
    ClusterNode oldest();

    /**
     * Returns the youngest node of this topology. Returns {@code null} if this topology is empty.
     *
     * <p>
     * The youngest node is the node with the highest {@link ClusterNode#joinOrder() join order}.
     * </p>
     *
     * @return Youngest node within of this topology or {@code null} if this topology is empty.
     */
    ClusterNode youngest();

    /**
     * Returns a random node of this topology. Returns {@code null} if this topology is empty.
     *
     * @return Random node or {@code null} if this topology is empty.
     */
    ClusterNode random();

    /**
     * Returns a copy of this topology that contains only those nodes that match the specified filter. The {@link #version() topology
     * version} will be preserved in the returned copy.
     *
     * @param filter Filter.
     *
     * @return Copy of this topology that contains only those nodes that match the specified filter.
     */
    ClusterTopology filterAll(ClusterFilter filter);

    /**
     * Returns a copy of this topology containing only those nodes that match the specified filter. The {@link #version() topology
     * version} will be preserved in the returned copy.
     *
     * @param filter Filter.
     *
     * @return Copy of this topology that contains only those nodes that match the specified filter.
     */
    ClusterTopology filter(ClusterNodeFilter filter);

    /**
     * Constructs a new cluster topology.
     *
     * @param version See {@link #version()}.
     * @param nodes See {@link #nodes()}.
     *
     * @return New topology.
     */
    static ClusterTopology of(long version, Set<ClusterNode> nodes) {
        ArgAssert.notNull(nodes, "Nodes");

        return DefaultClusterTopology.of(version, nodes);
    }

    /**
     * Return an empty cluster topology.
     *
     * @return Empty topology that has its {@link #version()} equals to {@code 0} and having an empty list of {@link #nodes()}.
     */
    static ClusterTopology empty() {
        return DefaultClusterTopology.empty();
    }
}
