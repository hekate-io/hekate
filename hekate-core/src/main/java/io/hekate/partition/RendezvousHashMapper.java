package io.hekate.partition;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologySupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Murmur3;
import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Implementation of {@link PartitionMapper} that uses
 * <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing" target="_blank">Rendezvous</a> (aka Highest Random Weight) hashing algorithm
 * for partition mapping.
 */
public final class RendezvousHashMapper implements PartitionMapper {
    /**
     * Builder for {@link RendezvousHashMapper}.
     *
     * @see RendezvousHashMapper#of(ClusterTopologySupport)
     */
    public static final class Builder {
        private final ClusterTopologySupport cluster;

        private int partitions = DEFAULT_PARTITIONS;

        private int backupNodes;

        private Builder(ClusterTopologySupport cluster) {
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
         * Value of this parameter must be above zero and must be a power of two. Default value is {@value #DEFAULT_PARTITIONS}.
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

        private int hash;

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

    private static class PartitionMapperSnapshot implements PartitionMapper {
        private final int backupSize;

        @ToStringIgnore
        private final ClusterTopology topology;

        @ToStringIgnore
        private final AtomicReferenceArray<Partition> partitions;

        @ToStringIgnore
        private final int maxPid;

        public PartitionMapperSnapshot(int size, int backupSize, ClusterTopology topology) {
            assert size > 0 : "Partitions size is less than or equals to zero [size=" + size + ']';
            assert Utils.isPowerOfTwo(size) : "Partitions size must be a power of two [size=" + size + ']';
            assert topology != null : "Topology is null.";

            this.backupSize = backupSize < 0 ? Integer.MAX_VALUE : backupSize;
            this.topology = topology;
            this.partitions = new AtomicReferenceArray<>(size);
            this.maxPid = size - 1;
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

            Partition partition = partitions.get(pid);

            if (partition == null) {
                if (topology.isEmpty()) {
                    partition = new DefaultPartition(pid, null, emptyList(), topology);
                } else {
                    PartitionHash[] hashes = topology.stream()
                        .map(node -> new PartitionHash(node, pid))
                        .sorted(PartitionHash.COMPARATOR)
                        .toArray(PartitionHash[]::new);

                    ClusterNode primary = hashes[0].node();

                    List<ClusterNode> backup;

                    if (backupSize > 0) {
                        int maxNodes = Math.min(backupSize, hashes.length - 1);

                        backup = new ArrayList<>(maxNodes);

                        for (int j = 1; j < maxNodes + 1; j++) {
                            backup.add(hashes[j].node());
                        }

                        backup = unmodifiableList(backup);
                    } else {
                        backup = emptyList();
                    }

                    partition = new DefaultPartition(pid, primary, backup, topology);
                }

                partitions.compareAndSet(pid, null, partition);
            }

            return partition;
        }

        @Override
        public int partitions() {
            return partitions.length();
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

    /** Default value (={@value}) for {@link Builder#withPartitions(int)}. */
    public static final int DEFAULT_PARTITIONS = 256;

    private static final AtomicReferenceFieldUpdater<RendezvousHashMapper, PartitionMapperSnapshot> SNAPSHOT = newUpdater(
        RendezvousHashMapper.class,
        PartitionMapperSnapshot.class,
        "snapshot"
    );

    private final int size;

    private final int backupSize;

    @ToStringIgnore
    private final ClusterTopologySupport cluster;

    @ToStringIgnore
    @SuppressWarnings("unused") // <-- Updated via AtomicReferenceFieldUpdater.
    private volatile PartitionMapperSnapshot snapshot;

    private RendezvousHashMapper(int size, int backupSize, ClusterTopologySupport cluster) {
        ArgAssert.isTrue(size > 0, "Partitions size is less than or equals to zero [size=" + size + ']');
        ArgAssert.isTrue(Utils.isPowerOfTwo(size), "Partitions size must be a power of two [size=" + size + ']');
        ArgAssert.notNull(cluster, "cluster");

        this.size = size;
        this.backupSize = backupSize;
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
        ArgAssert.notNull(cluster, "cluster");

        return new Builder(cluster);
    }

    /**
     * Returns a copy of this mapper that will use the specified cluster topology and will inherit all other configuration options from this
     * instance.
     *
     * @param cluster Cluster topology.
     *
     * @return New mapper.
     */
    public RendezvousHashMapper copy(ClusterTopologySupport cluster) {
        return new RendezvousHashMapper(size, backupSize, cluster);
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
    public int partitions() {
        return size;
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

            if (localSnapshot != null && localSnapshot.topology().version() >= topology.version()) {
                return localSnapshot;
            } else {
                PartitionMapperSnapshot newSnapshot = new PartitionMapperSnapshot(size, backupSize, topology);

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
