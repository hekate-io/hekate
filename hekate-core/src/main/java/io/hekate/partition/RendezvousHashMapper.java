/*
 * Copyright 2020 The Hekate Project
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
import io.hekate.cluster.ClusterTopologySupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Murmur3;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Implementation of {@link PartitionMapper} that uses
 * <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing" target="_blank">Rendezvous</a> (aka Highest Random Weight) hashing algorithm
 * for partition mapping.
 */
public final class RendezvousHashMapper extends PartitionMapperBase {
    /**
     * Builder for {@link RendezvousHashMapper}.
     *
     * @see RendezvousHashMapper#of(ClusterTopologySupport)
     */
    public static final class Builder {
        private final ClusterTopologySupport cluster;

        private int partitions = DEFAULT_PARTITIONS;

        private int backupNodes;

        Builder(ClusterTopologySupport cluster) {
            this.cluster = cluster;
        }

        /**
         * Constructs a new mapper.
         *
         * @return New mapper.
         */
        public RendezvousHashMapper build() {
            return new RendezvousHashMapper(partitions, backupNodes, cluster);
        }

        /**
         * Sets the total amount of partitions that should be managed by the mapper (value must be a power of two).
         *
         * <p>
         * Value of this parameter must be above zero and must be a power of two.
         * Default value is {@link RendezvousHashMapper#DEFAULT_PARTITIONS}.
         * </p>
         *
         * @param partitions Total amount of partitions that should be managed by the mapper (value must be a power of two).
         *
         * @return This instance.
         */
        public Builder withPartitions(int partitions) {
            this.partitions = partitions;

            return this;
        }

        /**
         * Sets the amount of backup nodes that should be assigned to each partition by the mapper.
         *
         * <p>
         * If value of this parameter is zero then mapper will not manage backup nodes and {@link Partition#backupNodes()} will return an
         * empty set. If value of this parameter is negative then mapper will use all available cluster nodes as backup nodes.
         * </p>
         *
         * @param backupNodes Amount of backup nodes that should be assigned to each partition by the mapper.
         *
         * @return This instance.
         *
         * @see Partition#backupNodes()
         */
        public Builder withBackupNodes(int backupNodes) {
            this.backupNodes = backupNodes;

            return this;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static class PartitionHash {
        public static final Comparator<PartitionHash> COMPARATOR = (h1, h2) -> {
            int cmp = Integer.compare(h1.hash(), h2.hash());

            if (cmp == 0) {
                return h1.node().id().compareTo(h2.node().id());
            }

            return cmp;
        };

        private final ClusterNode node;

        private final int hash;

        public PartitionHash(ClusterNode node, int pid) {
            this.node = node;

            this.hash = Murmur3.hash(node.id().hashCode(), pid);
        }

        public int hash() {
            return hash;
        }

        public ClusterNode node() {
            return node;
        }
    }

    private static class Snapshot extends PartitionMapperBase {
        @ToStringIgnore
        private final ClusterTopology topology;

        @ToStringIgnore
        private final Partition[] partitions;

        public Snapshot(int partitions, int backupSize, ClusterTopology topology) {
            super(partitions, backupSize);

            this.topology = topology;
            this.partitions = new Partition[partitions];
        }

        @Override
        public Partition partition(int pid) {
            // No synchronization on partitions.
            // ---------------------------------------------------------------------
            // All operations are idempotent and results are immutable.
            // Therefore, multiple threads can overwrite the same value in parallel.
            Partition partition = partitions[pid];

            if (partition == null) {
                int topologySize = topology.size();

                if (topologySize > 1) {
                    // Find primary node.
                    PartitionHash[] hashes = topology.stream()
                        .map(node -> new PartitionHash(node, pid))
                        .sorted(PartitionHash.COMPARATOR)
                        .toArray(PartitionHash[]::new);

                    ClusterNode primary = hashes[0].node();

                    // Find backup nodes.
                    int maxBackups = Math.min(backupNodes(), hashes.length - 1);

                    List<ClusterNode> backup;

                    if (maxBackups > 0) {
                        backup = new ArrayList<>(maxBackups);

                        // Try to find nodes that are on different hosts.
                        Map<InetAddress, ClusterNode> differentHosts = new HashMap<>(maxBackups, 1.0f);

                        for (int i = 1; i < hashes.length && differentHosts.size() < maxBackups; i++) {
                            ClusterNode node = hashes[i].node();

                            if (!primary.socket().getAddress().equals(node.socket().getAddress())) {
                                differentHosts.putIfAbsent(node.socket().getAddress(), node);
                            }
                        }

                        backup.addAll(differentHosts.values());

                        // If couldn't gather enough backup nodes then add whatever other nodes are available.
                        if (backup.size() < maxBackups) {
                            Set<ClusterNode> usedNodes = new HashSet<>(backup);

                            for (int i = 1; backup.size() < maxBackups && i < hashes.length; i++) {
                                if (!usedNodes.contains(hashes[i].node())) {
                                    backup.add(hashes[i].node());
                                }
                            }
                        }

                        backup = unmodifiableList(backup);
                    } else {
                        backup = emptyList();
                    }

                    partition = new DefaultPartition(pid, primary, backup, topology);
                } else if (topologySize == 1) {
                    // Single node topology.
                    partition = new DefaultPartition(pid, topology.first(), emptyList(), topology);
                } else {
                    // Empty topology.
                    partition = new DefaultPartition(pid, null, emptyList(), topology);
                }

                partitions[pid] = partition;
            }

            return partition;
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
        public PartitionMapper copy(ClusterTopologySupport cluster) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Snapshot doesn't support copying.");
        }

        @Override
        public String toString() {
            return ToString.format(RendezvousHashMapper.class, this);
        }
    }

    /** Default value (={@value}) for {@link Builder#withPartitions(int)}. */
    public static final int DEFAULT_PARTITIONS = 256;

    private static final AtomicReferenceFieldUpdater<RendezvousHashMapper, Snapshot> SNAPSHOT = newUpdater(
        RendezvousHashMapper.class,
        Snapshot.class,
        "snapshot"
    );

    @ToStringIgnore
    private final ClusterTopologySupport cluster;

    @ToStringIgnore
    @SuppressWarnings("unused") // <-- Updated via AtomicReferenceFieldUpdater.
    private volatile Snapshot snapshot;

    RendezvousHashMapper(int partitions, int backupSize, ClusterTopologySupport cluster) {
        super(partitions, backupSize);

        ArgAssert.notNull(cluster, "Cluster");

        this.cluster = cluster;
    }

    /**
     * Constructs a new builder that can be used to configure {@link RendezvousHashMapper}.
     *
     * @param cluster Cluster topology that should be used for partitions mapping.
     *
     * @return Builder.
     *
     * @see Builder#build()
     */
    public static Builder of(ClusterTopologySupport cluster) {
        ArgAssert.notNull(cluster, "Cluster");

        return new Builder(cluster);
    }

    /**
     * Constructs a new mapper.
     *
     * @param cluster Cluster topology that should be used for partitions mapping.
     * @param partitions See {@link Builder#withPartitions(int)}
     * @param backupNodes See {@link Builder#withBackupNodes(int)}
     *
     * @return New mapper.
     */
    public static RendezvousHashMapper of(ClusterTopologySupport cluster, int partitions, int backupNodes) {
        return new RendezvousHashMapper(partitions, backupNodes, cluster);
    }

    @Override
    public RendezvousHashMapper copy(ClusterTopologySupport cluster) {
        return new RendezvousHashMapper(partitions(), backupNodes(), cluster);
    }

    @Override
    public Partition map(Object key) {
        return snapshot().map(key);
    }

    @Override
    public Partition mapInt(int key) {
        return snapshot().map(key);
    }

    @Override
    public Partition partition(int id) {
        return snapshot().partition(id);
    }

    @Override
    public ClusterTopology topology() {
        return snapshot().topology();
    }

    @Override
    public PartitionMapper snapshot() {
        while (true) {
            Snapshot localSnapshot = this.snapshot;

            ClusterTopology topology = cluster.topology();

            if (localSnapshot != null && localSnapshot.topology().version() >= topology.version()) {
                return localSnapshot;
            } else {
                Snapshot newSnapshot = new Snapshot(partitions(), backupNodes(), topology);

                if (SNAPSHOT.compareAndSet(this, localSnapshot, newSnapshot)) {
                    return newSnapshot;
                }
            }
        }
    }

    @Override
    public boolean isSnapshot() {
        return false;
    }

    @Override
    public String toString() {
        return ToString.format(PartitionMapper.class, this);
    }
}
