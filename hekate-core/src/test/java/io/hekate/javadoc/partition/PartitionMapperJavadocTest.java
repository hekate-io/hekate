package io.hekate.javadoc.partition;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import org.junit.Test;

public class PartitionMapperJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleUsage() throws Exception {
        Hekate hekate = createNode().join();

        // Start:usage
        // Prepare mapper.
        PartitionMapper mapper = RendezvousHashMapper.of(hekate.cluster())
            .withPartitions(128)
            .withBackupNodes(2)
            .build();

        // Map some imaginary data key to a partition.
        Partition partition = mapper.map("someDataKey1");

        System.out.println("Primary node: " + partition.primaryNode());
        System.out.println("Backup nodes: " + partition.backupNodes());
        // End:usage
    }
}
