/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.partition;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import java.util.List;

/**
 * Data partition.
 *
 * <p>
 * This interface provides information about the {@link #primaryNode() primary} and {@link #backupNodes() backup} nodes that were selected
 * by the {@link PartitionMapper partition mapper} based on its cluster topology view.
 * </p>
 *
 * <p>
 * Note that instances of this interface are immutable and will not change once obtained from the {@link PartitionMapper}.
 * </p>
 *
 * @see PartitionMapper#map(Object)
 */
public interface Partition extends Comparable<Partition> {
    /**
     * Returns the partition identifier.
     *
     * @return Partition identifier.
     */
    int id();

    /**
     * Returns the primary node that is assigned to this partition.
     *
     * @return Primary node.
     */
    ClusterNode primaryNode();

    /**
     * Returns {@code true} if the specified node is primary for this partition (see {@link #primaryNode()}).
     *
     * @param node Node.
     *
     * @return {@code true} if the specified node is primary for this partition (see {@link #primaryNode()}).
     */
    boolean isPrimary(ClusterNode node);

    /**
     * Returns {@code true} if the specified node is primary for this partition (see {@link #primaryNode()}).
     *
     * @param node Node identifier.
     *
     * @return {@code true} if the specified node is primary for this partition (see {@link #primaryNode()}).
     */
    boolean isPrimary(ClusterNodeId node);

    /**
     * Returns the backup node set of this partition.
     *
     * @return Set of backup nodes or an empty set if there are no backup nodes.
     */
    List<ClusterNode> backupNodes();

    /**
     * Returns {@code true} if this partition has {@link #backupNodes()}.
     *
     * @return {@code true} if this partition has {@link #backupNodes()}.
     */
    boolean hasBackupNodes();

    /**
     * Returns the set of all nodes that are mapped to this partition (including {@link #primaryNode() primary} and {@link
     * #backupNodes() backup}).
     *
     * @return The set of all nodes that are mapped to this partition.
     */
    List<ClusterNode> nodes();

    /**
     * Returns the cluster topology that this partition was mapped to.
     *
     * @return Cluster topology that this partition was mapped to.
     */
    ClusterTopology topology();

    /**
     * Compares this partition with the specified one based on {@link #id()}  value.
     *
     * @param o Other partition.
     *
     * @return Result of {@link #id()} values comparison.
     */
    @Override
    int compareTo(Partition o);
}
