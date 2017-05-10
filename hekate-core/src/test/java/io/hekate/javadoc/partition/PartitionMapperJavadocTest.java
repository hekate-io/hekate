package io.hekate.javadoc.partition;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.partition.HrwPartitionMapper;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import org.junit.Test;

public class PartitionMapperJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleAccessService() throws Exception {
        Hekate hekate = createNode();

        // Start:usage
        // Prepare mapper.
        PartitionMapper mapper = HrwPartitionMapper.of(hekate.cluster())
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
