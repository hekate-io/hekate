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
import io.hekate.core.internal.util.Utils;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultPartitionMapper implements PartitionMapper {
    private static class PartitionMapperSnapshot implements PartitionMapper {
        private final String name;

        private final int partitionsSize;

        private final int backupSize;

        private final ClusterTopology topology;

        @ToStringIgnore
        private final Partition[] partitions;

        public PartitionMapperSnapshot(String name, int partitionsSize, int backupSize, ClusterTopology topology, Partition[] partitions) {
            assert name != null : "Name is null.";
            assert partitionsSize > 0 : "Partitions size is less than or equals to zero [size=" + partitionsSize + ']';
            assert partitions != null : "Partitions mapping is null.";
            assert topology != null : "Topology is null.";

            this.name = name;
            this.partitionsSize = partitionsSize;
            this.backupSize = backupSize;
            this.partitions = partitions;
            this.topology = topology;
        }

        @Override
        public Partition map(Object key) {
            ArgAssert.notNull(key, "Key");

            int partitionIdx = Utils.mod(key.hashCode(), partitionsSize);

            Partition partition = partitions[partitionIdx];

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
        public int partitions() {
            return partitionsSize;
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
        assert cluster != null : "Cluster is null.";

        this.name = name;
        this.partitionsSize = partitionsSize;
        this.backupSize = backupSize;
        this.cluster = cluster;

        this.lock = new ReentrantLock();

        // Check that MD5 is available.
        try {
            MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize MD5 message digest.", e);
        }
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
    public int partitions() {
        return partitionsSize;
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

                MessageDigest md5;

                try {
                    md5 = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("Failed to initialize MD5 message digest.", e);
                }

                Partition[] partitions = new Partition[partitionsSize];

                if (topology.isEmpty()) {
                    for (int pid = 0; pid < partitionsSize; pid++) {
                        partitions[pid] = new DefaultPartition(pid, null, Collections.emptyList(), topology);
                    }
                } else {
                    List<ClusterNode> nodes = new ArrayList<>(topology.nodes());

                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(buf);

                    for (int pid = 0; pid < partitionsSize; pid++) {
                        Map<ClusterNode, Long> hashes = new IdentityHashMap<>();

                        for (ClusterNode node : nodes) {
                            try {
                                out.writeInt(pid);
                                out.writeLong(node.id().loBits());
                                out.writeLong(node.id().hiBits());
                            } catch (IOException e) {
                                // Never happens.
                            }

                            md5.update(buf.toByteArray());

                            long hash = hashToLong(md5.digest());

                            hashes.put(node, hash);

                            buf.reset();
                        }

                        nodes.sort((o1, o2) -> {
                            Long h1 = hashes.get(o1);
                            Long h2 = hashes.get(o2);

                            return h1.compareTo(h2);
                        });

                        ClusterNode primary = nodes.get(0);

                        List<ClusterNode> backup;

                        if (backupSize > 0) {
                            int maxNodes = Math.min(backupSize, nodes.size() - 1);

                            backup = new ArrayList<>(maxNodes);

                            for (int j = 1; j < maxNodes + 1; j++) {
                                backup.add(nodes.get(j));
                            }

                            backup = Collections.unmodifiableList(backup);
                        } else {
                            backup = Collections.emptyList();
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

    private long hashToLong(byte[] hashBytes) {
        long hash = hashBytes[0] & 0xFF;

        for (int i = 1; i < 8; i++) {
            hash |= (hashBytes[i] & 0xFFL) << i * 8;
        }

        return hash;
    }

    @Override
    public String toString() {
        return ToString.format(PartitionMapper.class, this);
    }
}
