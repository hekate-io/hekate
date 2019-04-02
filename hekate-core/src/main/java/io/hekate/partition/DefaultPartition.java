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

package io.hekate.partition;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

class DefaultPartition implements Partition {
    private final int id;

    private final ClusterNode primary;

    private final List<ClusterNode> backup;

    @ToStringIgnore
    private final List<ClusterNode> nodes;

    @ToStringIgnore
    private final ClusterTopology topology;

    public DefaultPartition(int id, ClusterNode primary, List<ClusterNode> backup, ClusterTopology topology) {
        assert backup != null : "Backup nodes list is null.";
        assert topology != null : "Topology is null.";

        this.id = id;
        this.primary = primary;
        this.backup = backup;
        this.topology = topology;

        List<ClusterNode> nodes;

        if (primary == null) {
            nodes = emptyList();
        } else if (backup.isEmpty()) {
            nodes = singletonList(primary);
        } else {
            nodes = new ArrayList<>(backup.size() + 1);

            nodes.add(primary);
            nodes.addAll(backup);

            nodes = unmodifiableList(nodes);
        }

        this.nodes = nodes;
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
