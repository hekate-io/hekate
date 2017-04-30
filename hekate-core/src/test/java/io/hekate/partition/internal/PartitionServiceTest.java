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

package io.hekate.partition.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.PartitionMapperConfig;
import io.hekate.partition.PartitionService;
import io.hekate.partition.PartitionServiceFactory;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionServiceTest extends HekateNodeTestBase {
    private interface PartitionConfigurer {
        void configure(PartitionServiceFactory factory);
    }

    @Test
    public void testEmptyPartitions() throws Exception {
        PartitionService partitions = createNode().join().partitions();

        assertTrue(partitions.allMappers().isEmpty());

        assertFalse(partitions.hasMapper("no-such-mapper"));

        expect(IllegalArgumentException.class, () -> partitions.mapper("no-such-mapper"));

        assertTrue(partitions.toString().startsWith(PartitionService.class.getSimpleName()));
    }

    @Test
    public void testEmptyTopology() throws Exception {
        PartitionMapper mapper = createPartitionNode(c ->
            c.withMapper(new PartitionMapperConfig("test")
                .withFilter(node -> false)
                .withPartitions(128)
                .withBackupNodes(2)
            )
        ).join().partitions().mapper("test");

        assertNull(mapper.map("key").primaryNode());
        assertTrue(mapper.map("key").backupNodes().isEmpty());

        PartitionMapper snapshot = mapper.snapshot();

        assertNull(snapshot.map("key").primaryNode());
        assertTrue(snapshot.map("key").backupNodes().isEmpty());
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        HekateTestNode node = createNode(boot ->
            boot.withService(new PartitionServiceFactory()
                .withMapper(new PartitionMapperConfig("mapper1"))
                .withMapper(new PartitionMapperConfig("mapper2"))
            )
        ).join();

        assertTrue(node.partitions().hasMapper("mapper1"));
        assertTrue(node.partitions().hasMapper("mapper2"));

        PartitionMapper mapper1 = node.partitions().mapper("mapper1");
        PartitionMapper mapper2 = node.partitions().mapper("mapper2");

        assertNotNull(mapper1);
        assertNotNull(mapper2);

        assertEquals(2, node.partitions().allMappers().size());
        assertTrue(node.partitions().allMappers().contains(mapper1));
        assertTrue(node.partitions().allMappers().contains(mapper2));
    }

    @Test
    public void testMapper() throws Exception {
        HekateTestNode node = createPartitionNode(c -> {
            PartitionMapperConfig cfg = new PartitionMapperConfig();

            cfg.setName("test");
            cfg.setBackupNodes(2);
            cfg.setPartitions(128);

            c.withMapper(cfg);
        }).join();

        PartitionMapper mapper = node.partitions().mapper("test");

        assertEquals(128, mapper.partitions().size());
        assertEquals(2, mapper.backupNodes());
        assertEquals("test", mapper.name());
        assertFalse(mapper.isSnapshot());
        assertEquals(node.localNode(), mapper.map("key").primaryNode());
        assertTrue(mapper.map("key").backupNodes().isEmpty());
        assertEquals(node.cluster().topology(), mapper.topology());
        assertEquals(ToString.format(PartitionMapper.class, mapper), mapper.toString());

        PartitionMapper snapshot = mapper.snapshot();

        assertEquals(128, snapshot.partitions().size());
        assertEquals(2, snapshot.backupNodes());
        assertEquals("test", snapshot.name());
        assertTrue(snapshot.isSnapshot());
        assertEquals(node.localNode(), mapper.map("key").primaryNode());
        assertTrue(snapshot.map("key").backupNodes().isEmpty());
        assertSame(snapshot, snapshot.snapshot());
        assertEquals(node.cluster().topology(), snapshot.topology());
        assertEquals(ToString.format(PartitionMapper.class, snapshot), snapshot.toString());

        Partition partition = mapper.map(Integer.MIN_VALUE);

        assertNotNull(partition);
        assertEquals(0, partition.id());
        assertEquals(node.localNode(), partition.primaryNode());
        assertTrue(partition.isPrimary(node.localNode()));
        assertTrue(partition.isPrimary(node.localNode().id()));
        assertTrue(partition.backupNodes().isEmpty());
        assertEquals(Collections.singletonList(node.localNode()), partition.nodes());
        assertEquals(node.cluster().topology(), partition.topology());
        assertEquals(partition.id(), partition.hashCode());
        assertEquals(partition, partition);
        assertFalse(partition.equals(new Object()));

        Partition otherPartition = mapper.map(1);

        assertEquals(1, otherPartition.id());
        assertNotEquals(partition, otherPartition);
        assertEquals(-1, partition.compareTo(otherPartition));
    }

    @Test
    public void testUnknownMapper() throws Exception {
        try {
            createPartitionNode().join().partitions().mapper("unknown");

            fail("Error was expected.");
        } catch (IllegalArgumentException e) {
            assertEquals("No such mapper [name=unknown]", e.getMessage());
        }
    }

    @Test
    public void testMapping() throws Exception {
        repeat(5, i -> {
            int partitions = 256;
            int values = 10000;

            List<HekateTestNode> nodes = new ArrayList<>();

            for (int j = 0; j < 5; j++) {
                nodes.add(createPartitionNode(c -> {
                    PartitionMapperConfig cfg = new PartitionMapperConfig();

                    cfg.setName("test" + i);
                    cfg.setBackupNodes(i);
                    cfg.setPartitions(partitions);

                    c.withMapper(cfg);
                }).join());
            }

            awaitForTopology(nodes);

            int[] distributions = new int[partitions];

            for (int j = 0; j < values; j++) {
                Partition first = null;

                for (Hekate node : nodes) {
                    PartitionMapper mapper = node.partitions().mapper("test" + i);

                    assertEquals("test" + i, mapper.name());
                    assertEquals(partitions, mapper.partitions().size());
                    assertEquals(i, mapper.backupNodes());

                    Partition partition = mapper.map(j);

                    if (first == null) {
                        first = partition;

                        distributions[first.id()]++;
                    } else {
                        assertEquals(first.id(), partition.id());
                        assertEquals(first.primaryNode(), partition.primaryNode());
                        assertEquals(first.backupNodes(), partition.backupNodes());
                        assertEquals(first.nodes(), partition.nodes());

                        assertEquals(i, partition.backupNodes().size());
                        assertEquals(i + 1, partition.nodes().size());
                    }
                }
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

            for (HekateTestNode node : nodes) {
                node.leave();
            }
        });
    }

    private HekateTestNode createPartitionNode() throws Exception {
        return createPartitionNode(null);
    }

    private HekateTestNode createPartitionNode(PartitionConfigurer configurer) throws Exception {
        return createNode(c -> {
            PartitionServiceFactory factory = new PartitionServiceFactory();

            if (configurer != null) {
                configurer.configure(factory);
            }

            c.withService(factory);
        });
    }
}
