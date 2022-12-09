/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToStringIgnore;

/**
 * Abstract base class for {@link PartitionMapper} implementations.
 */
public abstract class PartitionMapperBase implements PartitionMapper {
    private final int partitions;

    private final int backupNodes;

    @ToStringIgnore
    private final int maxPid;

    /**
     * Constructs a new instance.
     *
     * @param partitions See {@link #partitions()}.
     * @param backupNodes See {@link #backupNodes()}.
     */
    public PartitionMapperBase(int partitions, int backupNodes) {
        ArgAssert.positive(partitions, "Partitions");
        ArgAssert.powerOfTwo(partitions, "Partitions");

        this.partitions = partitions;
        this.backupNodes = backupNodes < 0 ? Integer.MAX_VALUE : backupNodes;
        this.maxPid = partitions - 1;
    }

    @Override
    public Partition map(Object key) {
        ArgAssert.notNull(key, "Key");

        return mapInt(key.hashCode());
    }

    @Override
    public Partition mapInt(int key) {
        int hash = (hash = key) ^ hash >>> 16;

        int pid = maxPid & hash;

        return partition(pid);
    }

    @Override
    public int partitions() {
        return partitions;
    }

    @Override
    public int backupNodes() {
        return backupNodes;
    }
}
