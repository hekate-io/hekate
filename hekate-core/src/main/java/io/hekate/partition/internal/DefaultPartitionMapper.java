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

package io.hekate.partition.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Murmur3;
import io.hekate.core.internal.util.Utils;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

class DefaultPartitionMapper implements PartitionMapper {
    private static class PartitionMapperSnapshot implements PartitionMapper {
        private final String name;

        private final int backupSize;

        private final ClusterTopology topology;

        @ToStringIgnore
        private final Partition[] partitions;

        private final List<Partition> partitionList;

        @ToStringIgnore
        private final int maxIdx;

        public PartitionMapperSnapshot(String name, int partitionsSize, int backupSize, ClusterTopology topology, Partition[] partitions) {
            assert name != null : "Name is null.";
            assert partitionsSize > 0 : "Partitions size is less than or equals to zero [size=" + partitionsSize + ']';
            assert Utils.isPowerOfTwo(partitionsSize) : "Partitions size must be a power of two [size=" + partitionsSize + ']';
            assert partitions != null : "Partitions mapping is null.";
            assert topology != null : "Topology is null.";

            this.name = name;
            this.backupSize = backupSize;
            this.partitions = partitions;
            this.topology = topology;
            this.maxIdx = partitionsSize - 1;

            this.partitionList = unmodifiableList(Arrays.asList(partitions));
        }

        @Override
        public Partition map(Object key) {
            ArgAssert.notNull(key, "Key");

            int hash = (hash = key.hashCode()) ^ hash >>> 16;

            Partition partition = partitions[maxIdx & hash];

            if (TRACE) {
                log.trace("Mapped key to a partition [mapper={}, key={}, partition={}]", name, key, partition);
            }

            return partition;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<Partition> partitions() {
            return partitionList;
        }

        @Override
        public int backupNodes() {
            return backupSize;
        }

        @Override
        public ClusterTopology topology() {
            return topology;
        }

        @Override
        public PartitionMapper snapshot() {
            return this;
        }

        @Override
        public boolean isSnapshot() {
            return true;
        }

        @Override
        public String toString() {
            return ToString.format(PartitionMapper.class, this);
        }
    }

    private static class NodeHash {
        public static final Comparator<NodeHash> COMPARATOR = Comparator.comparing(NodeHash::hash);

        private final ClusterNode clusterNode;

        private final int idHash;

        private int hash;

        public NodeHash(ClusterNode clusterNode) {
            this.clusterNode = clusterNode;
            this.idHash = clusterNode.id().hashCode();
        }

        public int hash() {
            return hash;
        }

        public void update(int pid) {
            this.hash = Murmur3.hash(idHash, pid);
        }

        public ClusterNode clusterNode() {
            return clusterNode;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionMapper.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final boolean TRACE = log.isTraceEnabled();

    private final String name;

    private final int partitionsSize;

    private final int backupSize;

    @ToStringIgnore
    private final ReentrantLock lock;

    @ToStringIgnore
    private final ClusterView cluster;

    @ToStringIgnore
    private volatile PartitionMapperSnapshot snapshot;

    public DefaultPartitionMapper(String name, int partitionsSize, int backupSize, ClusterView cluster) {
        assert name != null : "Name is null.";
        assert partitionsSize > 0 : "Partitions size is less than or equals to zero [size=" + partitionsSize + ']';
        assert Utils.isPowerOfTwo(partitionsSize) : "Partitions size must be a power of two [size=" + partitionsSize + ']';
        assert cluster != null : "Cluster is null.";

        this.name = name;
        this.partitionsSize = partitionsSize;
        this.backupSize = backupSize;
        this.cluster = cluster;

        this.lock = new ReentrantLock();
    }

    @Override
    public Partition map(Object key) {
        return snapshot().map(key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<Partition> partitions() {
        return snapshot().partitions();
    }

    @Override
    public int backupNodes() {
        return backupSize;
    }

    @Override
    public ClusterTopology topology() {
        return snapshot().topology();
    }

    @Override
    public PartitionMapper snapshot() {
        while (true) {
            PartitionMapperSnapshot localSnapshot = this.snapshot;

            ClusterTopology topology = cluster.topology();

            if (localSnapshot == null || localSnapshot.topology().version() < topology.version()) {
                update();
            } else {
                return localSnapshot;
            }
        }
    }

    @Override
    public boolean isSnapshot() {
        return false;
    }

    public void update() {
        lock.lock();

        try {
            ClusterTopology topology = cluster.topology();

            if (snapshot == null || snapshot.topology().version() < topology.version()) {
                if (DEBUG) {
                    log.debug("Updating cluster topology [mapper={}, topology={}]", this, topology);
                }

                Partition[] partitions = new Partition[partitionsSize];

                if (topology.isEmpty()) {
                    for (int pid = 0; pid < partitionsSize; pid++) {
                        partitions[pid] = new DefaultPartition(pid, null, emptyList(), topology);
                    }
                } else {
                    NodeHash[] hashes = topology.stream().map(NodeHash::new).toArray(NodeHash[]::new);

                    for (int pid = 0; pid < partitionsSize; pid++) {
                        for (NodeHash hash : hashes) {
                            hash.update(pid);
                        }

                        Arrays.sort(hashes, NodeHash.COMPARATOR);

                        ClusterNode primary = hashes[0].clusterNode();

                        List<ClusterNode> backup;

                        if (backupSize > 0) {
                            int maxNodes = Math.min(backupSize, hashes.length - 1);

                            backup = new ArrayList<>(maxNodes);

                            for (int j = 1; j < maxNodes + 1; j++) {
                                backup.add(hashes[j].clusterNode());
                            }

                            backup = unmodifiableList(backup);
                        } else {
                            backup = emptyList();
                        }

                        partitions[pid] = new DefaultPartition(pid, primary, backup, topology);
                    }
                }

                this.snapshot = new PartitionMapperSnapshot(name, partitionsSize, backupSize, topology, partitions);

                if (DEBUG) {
                    log.debug("Done updating cluster topology [mapper={}]", this);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return ToString.format(PartitionMapper.class, this);
    }
}
