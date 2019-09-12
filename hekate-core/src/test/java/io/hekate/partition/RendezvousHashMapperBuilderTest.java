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

import io.hekate.HekateTestBase;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.partition.RendezvousHashMapper.Builder;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RendezvousHashMapperBuilderTest extends HekateTestBase {
    private final Builder builder = RendezvousHashMapper.of(DefaultClusterTopology.empty());

    @Test
    public void testDefault() {
        PartitionMapper mapper = builder.build();

        assertEquals(RendezvousHashMapper.DEFAULT_PARTITIONS, mapper.partitions());
        assertEquals(0, mapper.backupNodes());
    }

    @Test
    public void testOptions() {
        PartitionMapper mapper = builder.withPartitions(RendezvousHashMapper.DEFAULT_PARTITIONS * 2)
            .withBackupNodes(100501)
            .build();

        assertEquals(RendezvousHashMapper.DEFAULT_PARTITIONS * 2, mapper.partitions());
        assertEquals(100501, mapper.backupNodes());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(builder), builder.toString());
    }
}
