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

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
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
    public void testSingleNodeMapping() throws Exception {
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
        assertEquals(ToString.format(RendezvousHashMapper.class, snapshot), snapshot.toString());

        Partition partition = mapper.map(Integer.MIN_VALUE);

        assertNotNull(partition);
        assertEquals(0, partition.id());
        assertEquals(node, partition.primaryNode());
        assertTrue(partition.isPrimary(node));
        assertTrue(partition.isPrimary(node.id()));
        assertTrue(partition.backupNodes().isEmpty());
        assertEquals(singletonList(node), partition.nodes());
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
    public void testMappingDistribution() throws Exception {
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

    @Test
    public void testBackupsResideOnDifferentHosts() throws Exception {
        Set<ClusterNode> nodes = new HashSet<>(asList(
            newNode("127.0.0.1", 1, "22a0310ac3b04a4a8920175d1874fba5"),
            newNode("127.0.0.1", 2, "39f2a564bcc64a40ab977deaadf1dd50"),
            newNode("127.0.0.2", 1, "435e757397654e3d89c0b5c9051c8f62"),
            newNode("127.0.0.2", 2, "4952491f27f64a2082a99567bcd1b8fa"),
            newNode("127.0.0.3", 1, "57414611d3384a3f9f2a8a53c8e92284"),
            newNode("127.0.0.4", 1, "7a2ead0f048947d7b0f08c53bdaa5c9d")
        ));

        DefaultClusterTopology topology = DefaultClusterTopology.of(1, nodes);

        PartitionMapper twoBackups = RendezvousHashMapper.of(topology)
            .withPartitions(128)
            .withBackupNodes(2)
            .build();

        Partition partition = twoBackups.map(1);

        assertNotNull(partition);
        assertEquals(1, partition.id());
        assertTrue(nodes.contains(partition.primaryNode()));
        assertEquals(2, partition.backupNodes().size());
        assertTrue(nodes.containsAll(partition.nodes()));

        assertTrue(
            "No backup node on the same host as primary node.",
            partition.backupNodes().stream().noneMatch(n ->
                n.address().socket().getAddress().equals(partition.primaryNode().address().socket().getAddress())
            )
        );

        assertEquals(
            "All nodes should be on different hosts.",
            partition.nodes().size(),
            partition.nodes().stream().map(it -> it.address().socket().getAddress()).distinct().count()
        );

        PartitionMapper fiveBackups = RendezvousHashMapper.of(topology)
            .withPartitions(128)
            .withBackupNodes(5)
            .build();

        assertEquals(5, fiveBackups.map(1).backupNodes().size());
    }

    private ClusterNode newNode(String host, int port, String id) throws Exception {
        return newNode(new ClusterNodeId(id), false, null, new ClusterAddress(new InetSocketAddress(host, port), new ClusterNodeId(id)));
    }
}
