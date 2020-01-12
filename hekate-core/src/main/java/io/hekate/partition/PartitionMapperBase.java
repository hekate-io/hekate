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
