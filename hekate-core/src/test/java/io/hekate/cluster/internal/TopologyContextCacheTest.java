/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.cluster.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterTopology;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class TopologyContextCacheTest extends HekateTestBase {
    private final TopologyContextCache cache = new TopologyContextCache();

    @Test
    public void test() throws Exception {
        Function<ClusterTopology, Object> supplier = topology -> new Object();

        ClusterTopology t1 = ClusterTopology.of(1, toSet(newNode()));
        ClusterTopology t2 = ClusterTopology.of(2, toSet(newNode(), newNode()));
        ClusterTopology t3 = ClusterTopology.of(3, toSet(newNode(), newNode(), newNode()));

        Object o1 = cache.get(t1, supplier);

        assertSame(o1, cache.get(t1, supplier));

        Object o2 = cache.get(t2, supplier);

        assertNotSame(o2, o1);
        assertSame(o2, cache.get(t2, supplier));

        Object o3 = cache.get(t3, supplier);

        assertNotSame(o1, o3);
        assertNotSame(o2, o3);
        assertSame(o3, cache.get(t3, supplier));
    }

    @Test
    public void testConcurrent() throws Exception {
        ClusterTopology[] topologies = {
            ClusterTopology.of(1, toSet(newNode())),
            ClusterTopology.of(2, toSet(newNode(), newNode())),
            ClusterTopology.of(3, toSet(newNode(), newNode(), newNode()))
        };

        runParallel(4, 1_000_000, status -> {
            ClusterTopology top = topologies[ThreadLocalRandom.current().nextInt(topologies.length)];

            assertEquals(top.hash(), cache.get(top, ClusterTopology::hash));
        });
    }
}
