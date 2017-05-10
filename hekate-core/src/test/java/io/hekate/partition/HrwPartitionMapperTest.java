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

package io.hekate.partition;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HrwPartitionMapperTest extends HekateNodeTestBase {
    @Test
    public void testEmptyTopology() throws Exception {
        PartitionMapper mapper = HrwPartitionMapper.of(DefaultClusterTopology.empty())
            .withPartitions(128)
            .withBackupNodes(2)
            .build();

        assertNull(mapper.map("key").primaryNode());
        assertTrue(mapper.map("key").backupNodes().isEmpty());

        PartitionMapper snapshot = mapper.snapshot();

        assertNull(snapshot.map("key").primaryNode());
        assertTrue(snapshot.map("key").backupNodes().isEmpty());
    }

    @Test
    public void testSingleNode() throws Exception {
        ClusterNode node = newNode();

        DefaultClusterTopology topology = DefaultClusterTopology.of(1, singleton(node));

        PartitionMapper mapper = HrwPartitionMapper.of(topology)
            .withPartitions(128)
            .withBackupNodes(2)
            .build();

        assertEquals(128, mapper.partitions());
        assertEquals(2, mapper.backupNodes());
        assertFalse(mapper.isSnapshot());
        assertEquals(node, mapper.map("key").primaryNode());
        assertTrue(mapper.map("key").backupNodes().isEmpty());
        assertEquals(topology, mapper.topology());
        assertEquals(ToString.format(PartitionMapper.class, mapper), mapper.toString());

        PartitionMapper snapshot = mapper.snapshot();

        assertEquals(128, snapshot.partitions());
        assertEquals(2, snapshot.backupNodes());
        assertTrue(snapshot.isSnapshot());
        assertEquals(node, mapper.map("key").primaryNode());
        assertTrue(snapshot.map("key").backupNodes().isEmpty());
        assertSame(snapshot, snapshot.snapshot());
        assertEquals(topology, snapshot.topology());
        assertEquals(ToString.format(PartitionMapper.class, snapshot), snapshot.toString());

        Partition partition = mapper.map(Integer.MIN_VALUE);

        assertNotNull(partition);
        assertEquals(0, partition.id());
        assertEquals(node, partition.primaryNode());
        assertTrue(partition.isPrimary(node));
        assertTrue(partition.isPrimary(node.id()));
        assertTrue(partition.backupNodes().isEmpty());
        assertEquals(Collections.singletonList(node), partition.nodes());
        assertEquals(topology, partition.topology());
        assertEquals(partition.id(), partition.hashCode());
        assertEquals(partition, partition);
        assertFalse(partition.equals(new Object()));

        Partition otherPartition = mapper.map(1);

        assertEquals(1, otherPartition.id());
        assertNotEquals(partition, otherPartition);
        assertEquals(-1, partition.compareTo(otherPartition));
    }

    @Test
    public void testMapping() throws Exception {
        repeat(5, i -> {
            int partitions = 256;
            int values = 10000;

            Set<ClusterNode> nodes = new HashSet<>();

            for (int j = 0; j < 5; j++) {
                nodes.add(newNode());
            }

            PartitionMapper mapper = HrwPartitionMapper.of(DefaultClusterTopology.of(1, nodes))
                .withPartitions(partitions)
                .withBackupNodes(i)
                .build();

            assertEquals(partitions, mapper.partitions());
            assertEquals(i, mapper.backupNodes());

            int[] distributions = new int[partitions];

            for (int j = 0; j < values; j++) {
                Partition partition = mapper.map(j);

                assertEquals(i, partition.backupNodes().size());
                assertEquals(i + 1, partition.nodes().size());

                distributions[partition.id()]++;
            }

            int lowBound = (int)((values / partitions) * 0.9);
            int highBound = (int)(values / partitions * 1.1);

            for (int j = 0; j < partitions; j++) {
                int distribution = distributions[j];

                assertTrue(
                    "real=" + distribution + ", min=" + lowBound + ", max=" + highBound,
                    distribution > lowBound && distribution < highBound
                );
            }
        });
    }
}
