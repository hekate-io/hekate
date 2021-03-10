/*
 * Copyright 2021 The Hekate Project
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
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import org.junit.Test;

import static java.util.Arrays.asList;
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

        mapper.update(topology, old -> new DefaultPartition(old.id(), n1, asList(n2, n3), topology));

        assertEquals(topology, mapper.topology());

        for (int i = 0; i < mapper.partitions(); i++) {
            Partition part = mapper.partition(i);

            assertEquals(i, part.id());
            assertEquals(n1, part.primaryNode());
            assertEquals(asList(n2, n3), part.backupNodes());
            assertEquals(asList(n1, n2, n3), part.nodes());
            assertEquals(topology, part.topology());
        }
    }
}
