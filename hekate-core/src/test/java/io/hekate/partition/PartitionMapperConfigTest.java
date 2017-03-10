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

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class PartitionMapperConfigTest extends HekateTestBase {
    private final PartitionMapperConfig cfg = new PartitionMapperConfig();

    @Test
    public void testName() {
        assertNull(cfg.getName());

        cfg.setName("test1");

        assertEquals("test1", cfg.getName());

        assertSame(cfg, cfg.withName("test2"));

        assertEquals("test2", cfg.getName());

        assertEquals("test3", new PartitionMapperConfig("test3").getName());
    }

    @Test
    public void testPartitions() {
        assertEquals(PartitionMapperConfig.DEFAULT_PARTITIONS, cfg.getPartitions());

        cfg.setPartitions(100001);

        assertEquals(100001, cfg.getPartitions());

        assertSame(cfg, cfg.withPartitions(100002));

        assertEquals(100002, cfg.getPartitions());
    }

    @Test
    public void testBackupNodes() {
        assertEquals(0, cfg.getBackupNodes());

        cfg.setBackupNodes(10001);

        assertEquals(10001, cfg.getBackupNodes());

        assertSame(cfg, cfg.withBackupNodes(10002));

        assertEquals(10002, cfg.getBackupNodes());
    }

    @Test
    public void testFilter() {
        assertNull(cfg.getFilter());

        ClusterNodeFilter f1 = mock(ClusterNodeFilter.class);
        ClusterNodeFilter f2 = mock(ClusterNodeFilter.class);

        cfg.setFilter(f1);

        assertSame(f1, cfg.getFilter());

        assertSame(cfg, cfg.withFilter(f2));

        assertSame(f2, cfg.getFilter());
    }
}
