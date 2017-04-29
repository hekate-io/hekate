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

package io.hekate.javadoc.partition;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.PartitionMapperConfig;
import io.hekate.partition.PartitionService;
import io.hekate.partition.PartitionServiceFactory;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class PartitionServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleAccessService() throws Exception {
        // Start:configure
        // Prepare partition service factory.
        PartitionServiceFactory factory = new PartitionServiceFactory();

        // Register some mappers.
        factory.withMapper(new PartitionMapperConfig()
            .withName("mapper1")
            .withPartitions(1024)
            .withBackupNodes(2));

        factory.withMapper(new PartitionMapperConfig()
            .withName("mapper2")
            .withPartitions(512)
            .withBackupNodes(0));

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        PartitionService partitions = hekate.partitions();
        // End:access

        assertNotNull(partitions);

        // Start:usage
        // Get mapper by its name.
        PartitionMapper mapper = hekate.partitions().mapper("mapper1");

        // Map some imaginary data key to a partition.
        Partition partition = mapper.map("someDataKey1");

        System.out.println("Primary node: " + partition.primaryNode());
        System.out.println("Backup nodes: " + partition.backupNodes());
        // End:usage

        //Start:get_primary_id
        partition.primaryNode().id();
        //End:get_primary_id

        hekate.leave();
    }
}
