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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopologyHash;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import java.io.Serializable;
import java.util.Objects;

class LockMigrationKey implements Serializable {
    private static final long serialVersionUID = 1;

    private final ClusterNodeId node;

    private final ClusterTopologyHash topology;

    private final long id;

    public LockMigrationKey(ClusterNodeId node, ClusterTopologyHash topology, long id) {
        this.node = node;
        this.topology = topology;
        this.id = id;
    }

    public ClusterNodeId getNode() {
        return node;
    }

    public ClusterTopologyHash getTopology() {
        return topology;
    }

    public long getId() {
        return id;
    }

    public boolean isSameNode(ClusterNodeId other) {
        return node.equals(other);
    }

    public boolean isSameTopology(PartitionMapper mapper) {
        return mapper != null && mapper.getTopology().getHash().equals(topology);
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

        return id == that.id && Objects.equals(node, that.node) && Objects.equals(topology, that.topology);
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = 31 * result + node.hashCode();
        result = 31 * result + topology.hashCode();
        result = 31 * result + Long.hashCode(id);

        return result;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
