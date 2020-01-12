package io.hekate.partition;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UpdatablePartitionMapperTest extends HekateTestBase {
    @Test
    public void testEmpty() {
        UpdatablePartitionMapper mapper = new UpdatablePartitionMapper(16, 2);

        for (int i = 0; i < mapper.partitions(); i++) {
            Partition part = mapper.partition(i);

            assertEquals(i, part.id());
            assertNull(part.primaryNode());
            assertTrue(part.backupNodes().isEmpty());
            assertTrue(part.nodes().isEmpty());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        ClusterNode n1 = newNode();
        ClusterNode n2 = newNode();
        ClusterNode n3 = newNode();

        ClusterTopology topology = ClusterTopology.of(1, toSet(n1, n2, n3));

        UpdatablePartitionMapper mapper = new UpdatablePartitionMapper(16, 2);

        mapper.update(topology, old -> new DefaultPartition(old.id(), n1, Arrays.asList(n2, n3), topology));

        assertEquals(topology, mapper.topology());

        for (int i = 0; i < mapper.partitions(); i++) {
            Partition part = mapper.partition(i);

            assertEquals(i, part.id());
            assertEquals(n1, part.primaryNode());
            assertEquals(Arrays.asList(n2, n3), part.backupNodes());
            assertEquals(Arrays.asList(n1, n2, n3), part.nodes());
            assertEquals(topology, part.topology());
        }
    }
}
