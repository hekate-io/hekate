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

package io.hekate.partition;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import java.util.List;

/**
 * Data partition.
 *
 * <p>
 * This interface represents a data partition mapping information as it was provided by the {@link PartitionMapper#map(Object)} and
 * provides information about the {@link #getPrimaryNode() primary} and {@link #getBackupNodes() backup} nodes that were selected by the
 * mapper based on its cluster topology view.
 * </p>
 *
 * <p>
 * Note that instances of this interface are immutable and will not change once they are obtained from the {@link PartitionMapper}.
 * </p>
 *
 * <p>
 * For more details about partitions mapping please see the documentation of the {@link PartitionService} interface.
 * </p>
 *
 * @see PartitionMapper#map(Object)
 */
public interface Partition extends Comparable<Partition> {
    /**
     * Returns the partition identifier. The returned identifier value is always within the range [0..{@link
     * PartitionMapperConfig#getPartitions()}) range.
     *
     * @return Partition identifier.
     */
    int getId();

    /**
     * Returns the primary node that is assigned to this partition.
     *
     * @return Primary node.
     */
    ClusterNode getPrimaryNode();

    /**
     * Returns the primary node {@link ClusterNode#getId() identifier}.
     *
     * <p>
     * This method is just a shortcut for the following code:
     * ${source: partition/PartitionServiceJavadocTest.java#get_primary_id}
     * </p>
     *
     * @return primary node identifier (see {@link ClusterNode#getId()})
     */
    ClusterNodeId getPrimaryNodeId();

    /**
     * Returns {@code true} if the specified node is primary for this partition (see {@link #getPrimaryNode()}).
     *
     * @param node Node.
     *
     * @return {@code true} if the specified node is primary for this partition (see {@link #getPrimaryNode()}).
     */
    boolean isPrimary(ClusterNode node);

    /**
     * Returns {@code true} if the specified node is primary for this partition (see {@link #getPrimaryNode()}).
     *
     * @param node Node identifier.
     *
     * @return {@code true} if the specified node is primary for this partition (see {@link #getPrimaryNode()}).
     */
    boolean isPrimary(ClusterNodeId node);

    /**
     * Returns the set of backup nodes for this partition.
     *
     * <p>
     * The size of the returned set is controlled by the {@link PartitionMapperConfig#setBackupNodes(int)} configuration property. Returns
     * an empty set if this configuration property is less than or equals to zero.
     * </p>
     *
     * @return Set of backup nodes for this partition.
     *
     * @see PartitionMapperConfig#setBackupNodes(int)
     */
    List<ClusterNode> getBackupNodes();

    /**
     * Returns the set of all nodes that are mapped to this partition (including {@link #getPrimaryNode() primary} and {@link
     * #getBackupNodes() backup}).
     *
     * @return The set of all nodes that are mapped to this partition.
     */
    List<ClusterNode> getNodes();

    /**
     * Returns the cluster topology that this partition was mapped to.
     *
     * @return Cluster topology that this partition was mapped to.
     */
    ClusterTopology getTopology();

    /**
     * Compares this partition with the specified one based on {@link #getId()}  value.
     *
     * @param o Other partition.
     *
     * @return Result of {@link #getId()} values comparison.
     */
    @Override
    int compareTo(Partition o);
}
