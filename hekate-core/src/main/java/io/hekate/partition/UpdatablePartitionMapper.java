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

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologySupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Updatable implementation of {@link PartitionMapper} interface.
 *
 * <p>
 * This class is intended for use cases when it is required to manually control partitions mapping (as opposed to automatic mapping that is
 * provided by the {@link RendezvousHashMapper}).
 * </p>
 *
 * <p>
 * Partitions mapping can be updated via the {@link #update(ClusterTopology, Function)} method.
 * </p>
 */
public class UpdatablePartitionMapper extends PartitionMapperBase {
    private static final class Snapshot extends PartitionMapperBase {
        private final ClusterTopology topology;

        private final Partition[] partitions;

        public Snapshot(ClusterTopology topology, Partition[] partitions, int backupSize) {
            super(partitions.length, backupSize);

            this.topology = topology;
            this.partitions = partitions;
        }

        public static Snapshot empty(int partitions, int backupSize) {
            return new Snapshot(
                ClusterTopology.empty(),
                IntStream.range(0, partitions)
                    .mapToObj(DefaultPartition::empty)
                    .toArray(Partition[]::new),
                backupSize
            );
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
        public Partition partition(int id) {
            return partitions[id];
        }

        @Override
        public ClusterTopology topology() {
            return topology;
        }

        @Override
        public PartitionMapper copy(ClusterTopologySupport cluster) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Snapshot doesn't support copying.");
        }

        @Override
        public String toString() {
            return ToString.format(UpdatablePartitionMapper.class, this);
        }
    }

    @ToStringIgnore
    private volatile Snapshot snapshot;

    /**
     * Constructs a new instance.
     *
     * @param partitions See {@link #partitions()}.
     * @param backupSize See {@link #backupNodes()}.
     */
    public UpdatablePartitionMapper(int partitions, int backupSize) {
        super(partitions, backupSize);

        snapshot = Snapshot.empty(partitions, backupSize);
    }

    /**
     * Updates partitions mapping by applying the specified {@code update} function to each partition and using its result as a new mapping.
     *
     * @param topology New cluster topology (see {@link #topology()}).
     * @param update Partitions update function.
     */
    public void update(ClusterTopology topology, Function<Partition, Partition> update) {
        ArgAssert.notNull(topology, "Topology");
        ArgAssert.notNull(update, "Update function");

        Snapshot oldSnapshot = this.snapshot;

        Partition[] newPartitions = new Partition[partitions()];

        for (int i = 0; i < newPartitions.length; i++) {
            Partition newPartition = update.apply(oldSnapshot.partition(i));

            Objects.requireNonNull(newPartition, "Partition mapper function returned null.");

            newPartitions[i] = newPartition;
        }

        this.snapshot = new Snapshot(topology, newPartitions, backupNodes());
    }

    @Override
    public Partition partition(int id) {
        return snapshot.partition(id);
    }

    @Override
    public PartitionMapper snapshot() {
        return snapshot;
    }

    @Override
    public boolean isSnapshot() {
        return false;
    }

    @Override
    public ClusterTopology topology() {
        return snapshot().topology();
    }

    @Override
    public PartitionMapper copy(ClusterTopologySupport cluster) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support copying.");
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
