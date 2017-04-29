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

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.util.format.ToString;

/**
 * Configuration for {@link PartitionMapper}.
 *
 * <p>
 * Instances of this class can be {@link PartitionServiceFactory#withMapper(PartitionMapperConfig) registered} within the {@link
 * PartitionServiceFactory} in order to make particular partition mapper available in the {@link PartitionService}.
 * </p>
 *
 * <p>
 * For more details about partitions mapping please see the documentation of the {@link PartitionService} interface.
 * </p>
 *
 * @see PartitionServiceFactory#withMapper(PartitionMapperConfig)
 */
public class PartitionMapperConfig {
    /** Default value (={@value}) for {@link #setPartitions(int)}. */
    public static final int DEFAULT_PARTITIONS = 256;

    private String name;

    private int partitions = DEFAULT_PARTITIONS;

    private int backupNodes;

    private ClusterNodeFilter filter;

    /**
     * Default constructor.
     */
    public PartitionMapperConfig() {
        // No-op.
    }

    /**
     * Constructs new instance.
     *
     * @param name Mapper name (see {@link #setName(String)}).
     */
    public PartitionMapperConfig(String name) {
        this.name = name;
    }

    /**
     * Returns the partition mapper name (see {@link #setName(String)}).
     *
     * @return Partition mapper name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the partition mapper name.
     *
     * <p>
     * This name can be used to obtain partition mapper from the {@link PartitionService}.
     * </p>
     *
     * @param name Partition mapper name.
     *
     * @see PartitionService#mapper(String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Fluent-style version of {@link #setName(String)}.
     *
     * @param name Partition mapper name.
     *
     * @return This instance.
     */
    public PartitionMapperConfig withName(String name) {
        setName(name);

        return this;
    }

    /**
     * Returns the total amount of partitions that should be managed by the mapper (see {@link #setPartitions(int)}).
     *
     * @return Total amount of partitions that should be managed by the mapper.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Sets the total amount of partitions that should be managed by the mapper.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_PARTITIONS}.
     * </p>
     *
     * @param partitions Total amount of partitions that should be managed by the mapper.
     */
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    /**
     * Fluent-style version of {@link #setPartitions(int)}.
     *
     * @param partitions Total amount of partitions that should be managed by the mapper.
     *
     * @return This instance.
     */
    public PartitionMapperConfig withPartitions(int partitions) {
        setPartitions(partitions);

        return this;
    }

    /**
     * Returns the amount of backup nodes that should be assigned to each partition by the mapper (see {@link #setBackupNodes(int)}).
     *
     * @return Amount of backup nodes that should be assigned to each partition by the mapper.
     */
    public int getBackupNodes() {
        return backupNodes;
    }

    /**
     * Sets the amount of backup nodes that should be assigned to each partition by the mapper.
     *
     * <p>
     * If value of this parameter is negative or equals to zero then mapper will not manage backup nodes and {@link
     * Partition#backupNodes()} will return an empty set.
     * </p>
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition by the mapper.
     *
     * @see Partition#backupNodes()
     */
    public void setBackupNodes(int backupNodes) {
        this.backupNodes = backupNodes;
    }

    /**
     * Fluent-style version of {@link #setBackupNodes(int)}.
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition by the mapper.
     *
     * @return This instance.
     */
    public PartitionMapperConfig withBackupNodes(int backupNodes) {
        setBackupNodes(backupNodes);

        return this;
    }

    /**
     * Returns the cluster node filter for  partitions mapping (see {@link #setFilter(ClusterNodeFilter)}).
     *
     * @return Cluster node filter.
     */
    public ClusterNodeFilter getFilter() {
        return filter;
    }

    /**
     * Sets the cluster node filter for partitions mapping.
     *
     * <p>
     * If filter is specified, then only node accepted by the filter will be used for partitions mapping. If filter is not specified then
     * all nodes in the cluster will be used for partitions mapping.
     * </p>
     *
     * @param filter Cluster node filter.
     */
    public void setFilter(ClusterNodeFilter filter) {
        this.filter = filter;
    }

    /**
     * Fluent-style version of {@link #setFilter(ClusterNodeFilter)}.
     *
     * @param filter Cluster node filter.
     *
     * @return This instance.
     */
    public PartitionMapperConfig withFilter(ClusterNodeFilter filter) {
        setFilter(filter);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
