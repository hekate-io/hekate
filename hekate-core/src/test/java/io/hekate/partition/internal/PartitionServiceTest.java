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

import io.hekate.HekateInstanceTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateTestInstance;
import io.hekate.partition.Partition;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.PartitionMapperConfig;
import io.hekate.partition.PartitionService;
import io.hekate.partition.PartitionServiceFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionServiceTest extends HekateInstanceTestBase {
    private interface PartitionConfigurer {
        void configure(PartitionServiceFactory factory);
    }

    @Test
    public void testEmptyPartitions() throws Exception {
        PartitionService partitions = createInstance(boot -> boot.withService(new PartitionServiceFactory())).join()
            .get(PartitionService.class);

        assertTrue(partitions.getMappers().isEmpty());

        assertFalse(partitions.has("no-such-mapper"));

        expect(IllegalArgumentException.class, () -> partitions.get("no-such-mapper"));
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        PartitionService partitions = createInstance(boot ->
            boot.withService(new PartitionServiceFactory()
                .withMapper(new PartitionMapperConfig("mapper1"))
                .withMapper(new PartitionMapperConfig("mapper2"))
            )
        ).join().get(PartitionService.class);

        assertTrue(partitions.has("mapper1"));
        assertTrue(partitions.has("mapper2"));

        PartitionMapper mapper1 = partitions.get("mapper1");
        PartitionMapper mapper2 = partitions.get("mapper2");

        assertNotNull(mapper1);
        assertNotNull(mapper2);

        assertEquals(2, partitions.getMappers().size());
        assertTrue(partitions.getMappers().contains(mapper1));
        assertTrue(partitions.getMappers().contains(mapper2));
    }

    @Test
    public void testPreConfiguredMapper() throws Exception {
        HekateTestInstance instance = createPartitionInstance(c -> {
            PartitionMapperConfig cfg = new PartitionMapperConfig();

            cfg.setName("test");
            cfg.setBackupNodes(2);
            cfg.setPartitions(10);

            c.withMapper(cfg);
        }).join();

        PartitionMapper mapper = instance.get(PartitionService.class).get("test");

        assertNotNull(mapper);

        Partition partition = mapper.map(1);

        assertNotNull(partition);
        assertEquals(instance.getNode(), partition.getPrimaryNode());
        assertTrue(partition.getBackupNodes().isEmpty());
        assertEquals(Collections.singletonList(instance.getNode()), partition.getNodes());
    }

    @Test
    public void testUnknownMapper() throws Exception {
        try {
            createPartitionInstance().join().get(PartitionService.class).get("unknown");

            fail("Error was expected.");
        } catch (IllegalArgumentException e) {
            assertEquals("No such mapper [name=unknown]", e.getMessage());
        }
    }

    @Test
    public void testRouting() throws Exception {
        repeat(5, i -> {
            int partitions = 10;
            int values = 1000;

            List<HekateTestInstance> instances = new ArrayList<>();

            for (int j = 0; j < 5; j++) {
                instances.add(createPartitionInstance(c -> {
                    PartitionMapperConfig cfg = new PartitionMapperConfig();

                    cfg.setName("test" + i);
                    cfg.setBackupNodes(i);
                    cfg.setPartitions(partitions);

                    c.withMapper(cfg);
                }).join());
            }

            awaitForTopology(instances);

            int[] distributions = new int[partitions];

            for (int j = 0; j < values; j++) {
                Partition first = null;

                for (Hekate instance : instances) {
                    PartitionMapper mapper = instance.get(PartitionService.class).get("test" + i);

                    assertEquals("test" + i, mapper.getName());
                    assertEquals(partitions, mapper.getPartitions());
                    assertEquals(i, mapper.getBackupNodes());

                    Partition partition = mapper.map(j);

                    if (first == null) {
                        first = partition;

                        distributions[first.getId()]++;
                    } else {
                        assertEquals(first.getId(), partition.getId());
                        assertEquals(first.getPrimaryNode(), partition.getPrimaryNode());
                        assertEquals(first.getBackupNodes(), partition.getBackupNodes());
                        assertEquals(first.getNodes(), partition.getNodes());

                        assertEquals(i, partition.getBackupNodes().size());
                        assertEquals(i + 1, partition.getNodes().size());
                    }
                }
            }

            int lowBound = (int)((values / partitions) * 0.8);
            int highBound = (int)(values / partitions * 1.2);

            for (int j = 0; j < partitions; j++) {
                int distribution = distributions[j];

                assertTrue(
                    "real=" + distribution + ", min=" + lowBound + ", max=" + highBound,
                    distribution > lowBound && distribution < highBound
                );
            }

            for (HekateTestInstance instance : instances) {
                instance.leave();
            }
        });
    }

    private HekateTestInstance createPartitionInstance() throws Exception {
        return createPartitionInstance(null);
    }

    private HekateTestInstance createPartitionInstance(PartitionConfigurer configurer) throws Exception {
        return createInstance(c -> {
            PartitionServiceFactory factory = new PartitionServiceFactory();

            if (configurer != null) {
                configurer.configure(factory);
            }

            c.withService(factory);
        });
    }
}
