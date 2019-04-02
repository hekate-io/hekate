/*
 * Copyright 2019 The Hekate Project
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
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RendezvousHashMapperTest extends HekateNodeTestBase {
    @Test
    public void testEmptyTopology() throws Exception {
        PartitionMapper mapper = RendezvousHashMapper.of(DefaultClusterTopology.empty())
            .withPartitions(128)
            .withBackupNodes(2)
            .build();

        assertNull(mapper.map("key").primaryNode());
        assertFalse(mapper.map("key").hasBackupNodes());
        assertTrue(mapper.map("key").backupNodes().isEmpty());

        PartitionMapper snapshot = mapper.snapshot();

        assertNull(snapshot.map("key").primaryNode());
        assertFalse(snapshot.map("key").hasBackupNodes());
        assertTrue(snapshot.map("key").backupNodes().isEmpty());
    }

    @Test
    public void testSingleNode() throws Exception {
        ClusterNode node = newNode();

        DefaultClusterTopology topology = DefaultClusterTopology.of(1, singleton(node));

        PartitionMapper mapper = RendezvousHashMapper.of(topology)
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
        assertEquals(ToString.format(Partition.class, partition), partition.toString());

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

            AtomicReference<ClusterTopology> topologyRef = new AtomicReference<>();

            PartitionMapper mapper = RendezvousHashMapper.of(topologyRef::get)
                .withPartitions(partitions)
                .withBackupNodes(i)
                .build();

            repeat(5, j -> {
                Set<ClusterNode> nodes = new HashSet<>();

                for (int k = 0; k < j + 1; k++) {
                    nodes.add(newNode());
                }

                topologyRef.set(DefaultClusterTopology.of(j + 1, nodes));

                assertEquals(partitions, mapper.partitions());
                assertEquals(i, mapper.backupNodes());

                int[] distributions = new int[partitions];

                for (int k = 0; k < values; k++) {
                    Partition partition = mapper.map(k);

                    assertSame(partition, mapper.partition(partition.id()));

                    assertEquals(Math.min(i, nodes.size() - 1), partition.backupNodes().size());
                    assertEquals(Math.min(i + 1, nodes.size()), partition.nodes().size());

                    distributions[partition.id()]++;
                }

                int lowBound = (int)((values / partitions) * 0.9);
                int highBound = (int)(values / partitions * 1.1);

                for (int k = 0; k < partitions; k++) {
                    int distribution = distributions[k];

                    assertTrue(
                        "real=" + distribution + ", min=" + lowBound + ", max=" + highBound,
                        distribution > lowBound && distribution < highBound
                    );
                }
            });
        });
    }
}
