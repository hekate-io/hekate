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

import io.hekate.cluster.ClusterTopology;
import java.util.List;

/**
 * Partition mapper.
 *
 * <p>
 * This interface represents a data partition mapper within the {@link PartitionService} and is responsible for mapping user-provided data
 * keys to cluster partitions. Instances of this interface can be obtained via the {@link PartitionService#mapper(String)} method.
 * </p>
 *
 * <p>
 * For more details about the {@link PartitionService} and its capabilities please see the documentation of {@link PartitionService}
 * interface.
 * </p>
 *
 * @see PartitionService#mapper(String)
 * @see PartitionServiceFactory#withMapper(PartitionMapperConfig)
 */
public interface PartitionMapper {
    /**
     * Maps the specified key to the {@link Partition}.
     *
     * <p>
     * Note that returned {@link Partition} instances are immutable and will not reflect changes in the cluster topology. Consider calling
     * this method with the same key in order to obtain the {@link Partition} instance with the latest cluster topology mapping.
     * </p>
     *
     * @param key Data key.
     *
     * @return Partition.
     *
     * @see Partition#primaryNode()
     * @see Partition#backupNodes()
     */
    Partition map(Object key);

    /**
     * Returns the name of this mapper.
     *
     * <p>
     * Mapper name can be configured via the {@link PartitionMapperConfig#setName(String)} method and can be used to obtain mappers via the
     * {@link PartitionService#mapper(String)} method.
     * </p>
     *
     * @return Name of this mapper.
     */
    String name();

    /**
     * Returns all partitions.
     *
     * <p>
     * Total amount of partitions is specified by the {@link PartitionMapperConfig#setPartitions(int)} configuration property.
     * </p>
     *
     * @return All partitions.
     */
    List<Partition> partitions();

    /**
     * Returns the amount of backup nodes that should be assigned to each partition.
     *
     * <p>
     * Backup nodes are specified by the {@link PartitionMapperConfig#setBackupNodes(int)} configuration property.
     * </p>
     *
     * @return Amount of backup nodes that should be assigned to each partition.
     */
    int backupNodes();

    /**
     * Returns the cluster topology as it is visible to this mapper.
     *
     * <p>
     * If mapper is a {@link #snapshot() snapshot} then this method will always return the same topology at the time when the snapshot
     * was created. If mapper is a live mapper then it will always return the latest cluster topology.
     * </p>
     *
     * @return Cluster topology.
     */
    ClusterTopology topology();

    /**
     * Creates a point in time copy of this mapper. The returned snapshot is immutable and will not change its partitions mapping even if
     * cluster topology changes.
     *
     * @return New snapshot or the same instance if this instance is already a snapshot.
     *
     * @see #isSnapshot()
     */
    PartitionMapper snapshot();

    /**
     * Returns {@code true} if this instance is a {@link #snapshot() snapshot}.
     *
     * @return {@code true} if this instance is a {@link #snapshot() snapshot}.
     *
     * @see #snapshot()
     */
    boolean isSnapshot();
}
