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

package io.hekate.cluster;

import io.hekate.HekateTestBase;
import java.util.Collections;
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

        assertEquals(1, t.getVersion());
        assertEquals(HASH_SIZE, t.getHash().getBytes().length);

        assertEquals(0, t.size());
        assertTrue(t.getNodes().isEmpty());
        assertTrue(t.getNodesList().isEmpty());
        assertTrue(t.getRemoteNodes().isEmpty());
        assertTrue(t.getJoinOrder().isEmpty());
        assertTrue(t.getSorted().isEmpty());
        assertFalse(t.contains(newNode()));
        assertFalse(t.contains(newNodeId()));
        assertFalse(t.iterator().hasNext());

        assertNull(t.getLocalNode());
        assertNull(t.getYoungest());
        assertNull(t.getOldest());
        assertNull(t.getRandom());
    }

    @Test
    public void testSingleLocalNode() throws Exception {
        ClusterNode n = newLocalNode(node -> node.withJoinOrder(1));

        ClusterTopology t = newTopology(1, Collections.singleton(n));

        assertEquals(1, t.getVersion());
        assertEquals(HASH_SIZE, t.getHash().getBytes().length);

        assertEquals(1, t.size());
        assertEquals(1, t.getNodes().size());
        assertEquals(1, t.getNodesList().size());
        assertTrue(t.getNodes().contains(n));

        assertEquals(0, t.getRemoteNodes().size());

        assertEquals(1, t.getJoinOrder().size());
        assertTrue(t.getJoinOrder().contains(n));
        assertEquals(1, t.getJoinOrderList().size());
        assertTrue(t.getJoinOrderList().contains(n));

        assertEquals(1, t.getSorted().size());
        assertTrue(t.getSorted().contains(n));
        assertEquals(1, t.getSortedList().size());
        assertTrue(t.getSortedList().contains(n));

        assertEquals(n, t.getLocalNode());
        assertEquals(n, t.getYoungest());
        assertEquals(n, t.getOldest());
        assertEquals(n, t.getRandom());

        assertNull(t.get(newNodeId()));
    }

    @Test
    public void testSingleRemoteNode() throws Exception {
        ClusterNode n = newNode(node -> node.withJoinOrder(1));

        ClusterTopology t = newTopology(1, Collections.singleton(n));

        assertEquals(1, t.getVersion());
        assertEquals(HASH_SIZE, t.getHash().getBytes().length);

        assertEquals(1, t.size());
        assertEquals(1, t.getNodes().size());
        assertEquals(1, t.getNodesList().size());
        assertTrue(t.getNodes().contains(n));

        assertEquals(1, t.getRemoteNodes().size());
        assertTrue(t.getRemoteNodes().contains(n));

        assertEquals(1, t.getJoinOrder().size());
        assertTrue(t.getJoinOrder().contains(n));
        assertEquals(1, t.getJoinOrderList().size());
        assertTrue(t.getJoinOrderList().contains(n));

        assertEquals(1, t.getSorted().size());
        assertTrue(t.getSorted().contains(n));
        assertEquals(1, t.getSortedList().size());
        assertTrue(t.getSortedList().contains(n));

        assertNull(t.getLocalNode());

        assertEquals(n, t.getYoungest());
        assertEquals(n, t.getOldest());
        assertEquals(n, t.getRandom());

        assertSame(n, t.get(n.getId()));
    }

    @Test
    public void testMultipleNodes() throws Exception {
        NavigableSet<ClusterNode> remoteNodes = new TreeSet<>();

        ClusterNode oldest = newNode(n -> n.withJoinOrder(1));

        remoteNodes.add(oldest);
        remoteNodes.add(newNode(n -> n.withJoinOrder(2)));
        remoteNodes.add(newNode(n -> n.withJoinOrder(3)));
        remoteNodes.add(newNode(n -> n.withJoinOrder(4)));

        NavigableSet<ClusterNode> allNodes = new TreeSet<>(remoteNodes);

        ClusterNode localNode = newLocalNode(n -> n.withJoinOrder(5));

        allNodes.add(localNode);

        ClusterTopology t = newTopology(1, allNodes);

        assertEquals(1, t.getVersion());
        assertEquals(HASH_SIZE, t.getHash().getBytes().length);

        assertEquals(allNodes.size(), t.size());
        assertEquals(allNodes.size(), t.getNodes().size());
        assertEquals(allNodes.size(), t.getNodesList().size());
        assertEquals(allNodes, t.getNodes());

        assertEquals(remoteNodes, t.getRemoteNodes());

        assertEquals(allNodes, t.getJoinOrder());
        assertEquals(oldest, t.getJoinOrder().first());
        assertEquals(localNode, t.getJoinOrder().last());
        assertEquals(oldest, t.getJoinOrderList().get(0));
        assertEquals(localNode, t.getJoinOrder().last());
        assertEquals(localNode, t.getJoinOrderList().get(t.getJoinOrderList().size() - 1));

        assertEquals(allNodes, t.getSorted());
        assertEquals(allNodes.first(), t.getSorted().first());
        assertEquals(allNodes.first(), t.getSortedList().get(0));
        assertEquals(allNodes.last(), t.getSorted().last());
        assertEquals(allNodes.last(), t.getSortedList().get(t.getSortedList().size() - 1));

        assertEquals(localNode, t.getLocalNode());

        assertEquals(localNode, t.getYoungest());
        assertEquals(oldest, t.getOldest());

        assertNotNull(t.getRandom());

        allNodes.forEach(n -> assertSame(n, t.get(n.getId())));
    }

    @Test
    public void testFilter() throws Exception {
        ClusterNode n1 = newNode(n -> n.withJoinOrder(1));
        ClusterNode n2 = newNode(n -> n.withJoinOrder(2));
        ClusterNode n3 = newNode(n -> n.withJoinOrder(3));

        ClusterTopology t1 = newTopology(1, toSet(n1, n2, n3));

        assertEquals(0, t1.filter(n -> n.getJoinOrder() > 10).size());

        ClusterTopology t2 = t1.filter(n -> n.getJoinOrder() >= 2);

        assertNotEquals(t1.getHash(), t2.getHash());

        assertEquals(t1.getVersion(), t2.getVersion());

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

        assertEquals(1, t1.getVersion());
        assertEquals(2, t2.getVersion());
    }
}
