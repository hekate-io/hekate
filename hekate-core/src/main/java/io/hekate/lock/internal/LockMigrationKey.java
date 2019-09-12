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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import java.util.Objects;

class LockMigrationKey {
    private final ClusterNodeId coordinator;

    private final long id;

    private final ClusterHash topology;

    public LockMigrationKey(ClusterNodeId coordinator, long id, ClusterHash topology) {
        this.coordinator = coordinator;
        this.topology = topology;
        this.id = id;
    }

    public ClusterNodeId coordinator() {
        return coordinator;
    }

    public ClusterHash topology() {
        return topology;
    }

    public long id() {
        return id;
    }

    public boolean isCoordinatedBy(ClusterNodeId nodeId) {
        return coordinator.equals(nodeId);
    }

    public boolean isSameTopology(PartitionMapper mapper) {
        return mapper != null && mapper.topology().hash().equals(topology);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof LockMigrationKey)) {
            return false;
        }

        LockMigrationKey that = (LockMigrationKey)o;

        return id == that.id && Objects.equals(coordinator, that.coordinator) && Objects.equals(topology, that.topology);
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = 31 * result + coordinator.hashCode();
        result = 31 * result + topology.hashCode();
        result = 31 * result + Long.hashCode(id);

        return result;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
