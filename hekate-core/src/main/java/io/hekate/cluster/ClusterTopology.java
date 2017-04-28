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

package io.hekate.cluster;

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
 * <li>{@link #getNodes()}</li>
 * <li>{@link #stream()}</li>
 * <li>{@link #forEach(Consumer)}</li>
 * <li>{@link #iterator()}</li>
 * <li>{@link #spliterator()}</li>
 * <li>{@link #getRemoteNodes()}</li>
 * <li>{@link #getFirst()}</li>
 * <li>{@link #getLast()}</li>
 * </ul>
 *
 * @see ClusterService#getTopology()
 */
public interface ClusterTopology extends Iterable<ClusterNode> {
    /**
     * Returns the topology version.
     *
     * <p>
     * This method is provided in order to enable optimistic checks on cluster topology changes. Value of this property is monotonically
     * incremented whenever a new snapshot is created due to cluster membership changes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Version counter is local to each node and can differ from node to node.
     * </p>
     *
     * @return Topology version.
     */
    long getVersion();

    /**
     * Returns the SHA-256 hash of all {@link ClusterNode#getId() cluster node identifiers} from this topology.
     *
     * <p>
     * If topology is {@link #filter(ClusterNodeFilter) filtered} then only nodes matching the filter will be used for hash calculation.
     * </p>
     *
     * @return SHA-256 hash of this topology.
     */
    ClusterTopologyHash getHash();

    /**
     * Returns local node or {@code null} if local node is not within this topology.
     *
     * @return Local node or {@code null} if local node is not within this topology.
     *
     * @see ClusterNode#isLocal()
     */
    ClusterNode getLocalNode();

    /**
     * Returns an immutable list of all nodes with consistent ordering based on {@link ClusterNode#compareTo(Object)} method. Returns an
     * empty list if there are no nodes within this topology.
     *
     * @return Returns the immutable list of all nodes with consistent ordering or an empty list if there are no nodes within this topology.
     */
    List<ClusterNode> getNodes();

    /**
     * Returns the first node of the {@link #getNodes()} list or {@code null} if this topology is empty.
     *
     * @return The first node of the {@link #getNodes()} list or {@code null} if this topology is empty.
     */
    ClusterNode getFirst();

    /**
     * Returns the last node of the {@link #getNodes()} list or {@code null} if this topology is empty.
     *
     * @return The last node of the {@link #getNodes()} list or {@code null} if this topology is empty.
     */
    ClusterNode getLast();

    /**
     * Returns an immutable set of all nodes within this topology. Returns an empty set if there are no nodes within this topology.
     *
     * @return Immutable set of all nodes within this topology or an empty list if there are no nodes within this topology.
     */
    Set<ClusterNode> getNodeSet();

    /**
     * Returns an immutable list of remote nodes within this topology ordered by their {@link ClusterNode#getId() identifiers}. Returns an
     * empty list if there are no remote nodes within this topology.
     *
     * @return Immutable list of remotes node within this topology ordered by their {@link ClusterNode#getId() identifiers} or an empty list
     * if there are no remote nodes within this topology..
     *
     * @see ClusterNode#isLocal()
     */
    List<ClusterNode> getRemoteNodes();

    /**
     * Returns an immutable set of all nodes ordered by their {@link ClusterNode#getJoinOrder() join order}. The first element in this set
     * is the oldest node. Returns an empty set if there are no nodes within this topology.
     *
     * @return Immutable set of all nodes ordered by their {@link ClusterNode#getJoinOrder() join order} or an empty set if there are no
     * nodes within this topology.
     */
    NavigableSet<ClusterNode> getJoinOrder();

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
     * @return node for the specified identifier or {@code null} if there is no such node within this topology.
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
     * Returns the oldest node from this topology. Returns {@code null} if there are no nodes within this topology.
     *
     * <p>
     * Oldest node is the node with the lowest {@link ClusterNode#getJoinOrder() join order} value.
     * </p>
     *
     * @return Oldest node from this topology or {@code null} if there are no nodes within this topology.
     */
    ClusterNode getOldest();

    /**
     * Returns the youngest node from this topology. Returns {@code null} if there are no nodes within this topology.
     *
     * <p>
     * Youngest node is the node with the highest {@link ClusterNode#getJoinOrder() join order} value.
     * </p>
     *
     * @return Youngest node within from this topology or {@code null} if there are no nodes within this topology.
     */
    ClusterNode getYoungest();

    /**
     * Returns a random node from this topology. Returns {@code null} if this topology doesn't contain any nodes.
     *
     * @return Random node or {@code null} if this topology is empty.
     */
    ClusterNode getRandom();

    /**
     * Returns a copy of this topology containing only those nodes that match the specified filter. The {@link #getVersion() topology
     * version} will be preserved in the returned copy.
     *
     * @param filter Filter.
     *
     * @return Copy of this topology containing only those nodes that match the specified filter.
     */
    ClusterTopology filterAll(ClusterFilter filter);

    /**
     * Returns a copy of this topology containing only those nodes that match the specified filter. The {@link #getVersion() topology
     * version} will be preserved in the returned copy.
     *
     * @param filter Filter.
     *
     * @return Copy of this topology containing only those nodes that match the specified filter.
     */
    ClusterTopology filter(ClusterNodeFilter filter);
}
