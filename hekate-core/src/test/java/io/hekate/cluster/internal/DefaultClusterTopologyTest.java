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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopologyTestBase;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultClusterTopologyTest extends ClusterTopologyTestBase {
    @Test
    public void testUpdate() throws Exception {
        ClusterNode n1 = newNode(n -> n.withJoinOrder(1));
        ClusterNode n2 = newNode(n -> n.withJoinOrder(2));
        ClusterNode n3 = newNode(n -> n.withJoinOrder(3));

        DefaultClusterTopology t1 = newTopology(1, Collections.singleton(n1));

        assertSame(t1, t1.updateIfModified(Collections.singleton(n1)));

        DefaultClusterTopology t2 = t1.updateIfModified(toSet(n1, n2, n3));

        assertEquals(2, t2.version());
        assertNotEquals(t1.hash(), t2.hash());

        assertTrue(t2.contains(n1));
        assertTrue(t2.contains(n2));
        assertTrue(t2.contains(n3));

        assertSame(t2, t2.updateIfModified(toSet(n1, n2, n3)));

        DefaultClusterTopology t3 = t2.updateIfModified(toSet(n1, n2));

        assertEquals(3, t3.version());
        assertNotEquals(t2.hash(), t3.hash());

        assertTrue(t3.contains(n1));
        assertTrue(t3.contains(n2));
        assertFalse(t3.contains(n3));

        DefaultClusterTopology t4 = t3.update(toSet(n1, n2));

        assertEquals(4, t4.version());
        assertEquals(t3.hash(), t4.hash());

        assertTrue(t4.contains(n1));
        assertTrue(t4.contains(n2));
        assertFalse(t4.contains(n3));

        DefaultClusterTopology t5 = t4.update(toSet(n1, n2, n3));

        assertEquals(5, t5.version());

        assertTrue(t5.contains(n1));
        assertTrue(t5.contains(n2));
        assertTrue(t5.contains(n3));
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        ClusterNode n1 = newNode(n -> n.withJoinOrder(1));
        ClusterNode n2 = newNode(n -> n.withJoinOrder(2));
        ClusterNode n3 = newNode(n -> n.withJoinOrder(3));

        // Same node set.
        DefaultClusterTopology t1 = newTopology(1, toSet(n1, n2, n3));
        DefaultClusterTopology t2 = newTopology(1, toSet(n1, n2, n3));

        // Same node set but different versions.
        DefaultClusterTopology t3 = newTopology(2, toSet(n1, n2, n3));

        // Other node set.
        DefaultClusterTopology t4 = newTopology(1, toSet(n1, n2));
        DefaultClusterTopology t5 = newTopology(1, Collections.emptySet());

        assertEquals(t1, t1);
        assertEquals(t1, t2);
        assertEquals(t1, t3);
        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1.hashCode(), t3.hashCode());

        assertNotEquals(t1, t4);
        assertNotEquals(t1, t5);
    }

    @Override
    protected DefaultClusterTopology newTopology(int version, Set<ClusterNode> nodes) {
        return DefaultClusterTopology.of(version, nodes);
    }
}
