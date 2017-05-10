package io.hekate.partition;

import io.hekate.HekateTestBase;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.partition.RendezvousHashMapper.Builder;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RendezvousHashMapperBuilderTest extends HekateTestBase {
    private final Builder builder = RendezvousHashMapper.of(DefaultClusterTopology.empty());

    @Test
    public void testDefault() {
        PartitionMapper mapper = builder.build();

        assertEquals(Builder.DEFAULT_PARTITIONS, mapper.partitions());
        assertEquals(0, mapper.backupNodes());
    }

    @Test
    public void testOptions() {
        PartitionMapper mapper = builder.withPartitions(Builder.DEFAULT_PARTITIONS * 2)
            .withBackupNodes(100501)
            .build();

        assertEquals(Builder.DEFAULT_PARTITIONS * 2, mapper.partitions());
        assertEquals(100501, mapper.backupNodes());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(builder), builder.toString());
    }
}
