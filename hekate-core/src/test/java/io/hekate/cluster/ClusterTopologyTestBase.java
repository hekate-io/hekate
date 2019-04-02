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

package io.hekate.cluster;

import io.hekate.HekateTestBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public abstract class ClusterTopologyTestBase extends HekateTestBase {
    private static final int HASH_SIZE = 32;

    protected abstract ClusterTopology newTopology(int version, Set<ClusterNode> nodes);

    @Test
    public void testEmpty() throws Exception {
        ClusterTopology t = newTopology(1, Collections.emptySet());

        assertEquals(1, t.version());
        assertEquals(HASH_SIZE, t.hash().bytes().length);

        assertEquals(0, t.size());
        assertTrue(t.nodes().isEmpty());
        assertTrue(t.nodeSet().isEmpty());
        assertTrue(t.remoteNodes().isEmpty());
        assertTrue(t.joinOrder().isEmpty());
        assertFalse(t.contains(newNode()));
        assertFalse(t.contains(newNodeId()));
        assertFalse(t.iterator().hasNext());

        assertNull(t.localNode());
        assertNull(t.youngest());
        assertNull(t.oldest());
        assertNull(t.random());
    }

    @Test
    public void testSingleLocalNode() throws Exception {
        ClusterNode n = newLocalNode(node -> node.withJoinOrder(1));

        ClusterTopology t = newTopology(1, Collections.singleton(n));

        assertEquals(1, t.version());
        assertEquals(HASH_SIZE, t.hash().bytes().length);

        assertEquals(1, t.size());
        assertEquals(1, t.nodes().size());
        assertTrue(t.nodes().contains(n));

        assertEquals(1, t.nodeSet().size());

        assertEquals(0, t.remoteNodes().size());

        assertEquals(1, t.joinOrder().size());
        assertTrue(t.joinOrder().contains(n));

        assertEquals(n, t.localNode());
        assertEquals(n, t.youngest());
        assertEquals(n, t.oldest());
        assertEquals(n, t.random());

        assertNull(t.get(newNodeId()));
    }

    @Test
    public void testSingleRemoteNode() throws Exception {
        ClusterNode n = newNode(node -> node.withJoinOrder(1));

        ClusterTopology t = newTopology(1, Collections.singleton(n));

        assertEquals(1, t.version());
        assertEquals(HASH_SIZE, t.hash().bytes().length);

        assertEquals(1, t.size());
        assertEquals(1, t.nodes().size());
        assertEquals(1, t.nodeSet().size());
        assertTrue(t.nodes().contains(n));

        assertEquals(1, t.remoteNodes().size());
        assertTrue(t.remoteNodes().contains(n));

        assertEquals(1, t.joinOrder().size());
        assertTrue(t.joinOrder().contains(n));

        assertNull(t.localNode());

        assertEquals(n, t.youngest());
        assertEquals(n, t.oldest());
        assertEquals(n, t.random());

        assertSame(n, t.get(n.id()));
    }

    @Test
    public void testMultipleNodes() throws Exception {
        List<ClusterNode> remoteNodes = new ArrayList<>();

        ClusterNode oldest = newNode(n -> n.withJoinOrder(1));

        remoteNodes.add(oldest);
        remoteNodes.add(newNode(n -> n.withJoinOrder(2)));
        remoteNodes.add(newNode(n -> n.withJoinOrder(3)));
        remoteNodes.add(newNode(n -> n.withJoinOrder(4)));

        Collections.sort(remoteNodes);

        NavigableSet<ClusterNode> allNodes = new TreeSet<>(remoteNodes);

        ClusterNode localNode = newLocalNode(n -> n.withJoinOrder(5));

        allNodes.add(localNode);

        ClusterTopology t = newTopology(1, allNodes);

        assertEquals(1, t.version());
        assertEquals(HASH_SIZE, t.hash().bytes().length);

        assertEquals(allNodes.size(), t.size());
        assertEquals(allNodes.size(), t.nodes().size());
        assertEquals(allNodes.size(), t.nodeSet().size());

        assertEquals(remoteNodes, t.remoteNodes());

        assertEquals(allNodes, t.joinOrder());
        assertEquals(oldest, t.joinOrder().first());
        assertEquals(localNode, t.joinOrder().last());
        assertEquals(localNode, t.joinOrder().last());

        assertEquals(new ArrayList<>(allNodes), t.nodes());
        assertEquals(allNodes.first(), t.nodes().get(0));
        assertEquals(allNodes.last(), t.nodes().get(t.nodes().size() - 1));

        assertEquals(localNode, t.localNode());

        assertEquals(localNode, t.youngest());
        assertEquals(oldest, t.oldest());

        assertNotNull(t.random());

        allNodes.forEach(n -> assertSame(n, t.get(n.id())));
    }

    @Test
    public void testFilter() throws Exception {
        ClusterNode n1 = newNode(n -> n.withJoinOrder(1));
        ClusterNode n2 = newNode(n -> n.withJoinOrder(2));
        ClusterNode n3 = newNode(n -> n.withJoinOrder(3));

        ClusterTopology t1 = newTopology(1, toSet(n1, n2, n3));

        assertEquals(0, t1.filter(n -> n.joinOrder() > 10).size());

        ClusterTopology t2 = t1.filter(n -> n.joinOrder() >= 2);

        assertNotEquals(t1.hash(), t2.hash());

        assertEquals(t1.version(), t2.version());

        assertEquals(2, t2.size());
        assertFalse(t2.contains(n1));
        assertTrue(t2.contains(n2));
        assertTrue(t2.contains(n3));

        assertSame(t1, t1.filter(n -> true));
    }

    @Test
    public void testVersion() throws Exception {
        ClusterTopology t1 = newTopology(1, toSet(newNode(), newNode(), newNode()));
        ClusterTopology t2 = newTopology(2, toSet(newNode(), newNode(), newNode()));

        assertEquals(1, t1.version());
        assertEquals(2, t2.version());
    }
}
