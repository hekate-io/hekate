/*
 * Copyright 2021 The Hekate Project
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
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * Default implementation of {@link PartitionMapper} interface.
 */
public class DefaultPartition implements Partition {
    /** See {@link #id()}. */
    private final int id;

    /** See {@link #primaryNode()}. */
    private final ClusterNode primary;

    /** See {@link #backupNodes()}. */
    private final List<ClusterNode> backup;

    /** See {@link #nodes()}. */
    @ToStringIgnore
    private final List<ClusterNode> nodes;

    /** See {@link #topology()}. */
    @ToStringIgnore
    private final ClusterTopology topology;

    /**
     * Constructs a new instance.
     *
     * @param id See {@link #id()}.
     * @param primary See {@link #primaryNode()}.
     * @param backup See {@link #backupNodes()}.
     * @param topology See {@link #topology()}.
     */
    public DefaultPartition(int id, ClusterNode primary, List<ClusterNode> backup, ClusterTopology topology) {
        ArgAssert.notNull(topology, "Topology");

        this.id = id;
        this.primary = primary;
        this.backup = Utils.nullSafeImmutableCopy(backup);
        this.topology = topology;

        this.nodes = combine(this.primary, this.backup);
    }

    /**
     * Returns a new instance that represents an empty partition.
     *
     * @param id Partition identifier.
     *
     * @return Empty partition.
     */
    public static DefaultPartition empty(int id) {
        return new DefaultPartition(id, null, null, ClusterTopology.empty());
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public ClusterNode primaryNode() {
        return primary;
    }

    @Override
    public List<ClusterNode> backupNodes() {
        return backup;
    }

    @Override
    public List<ClusterNode> nodes() {
        return nodes;
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    private static List<ClusterNode> combine(ClusterNode primary, List<ClusterNode> backup) {
        if (primary == null) {
            ArgAssert.check(backup == null || backup.isEmpty(), "Backup nodes can't be specified when primary node is null.");

            return emptyList();
        } else if (backup == null || backup.isEmpty()) {
            return singletonList(primary);
        } else {
            List<ClusterNode> nodes = new ArrayList<>(backup.size() + 1);

            nodes.add(primary);
            nodes.addAll(backup);

            return unmodifiableList(nodes);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Partition)) {
            return false;
        }

        Partition that = (Partition)o;

        return compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return ToString.format(Partition.class, this);
    }
}
