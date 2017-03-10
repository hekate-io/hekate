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
import io.hekate.coordinate.CoordinationService;
import io.hekate.messaging.MessagingService;
import io.hekate.network.NetworkService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class ClusterFiltersTest extends HekateTestBase {
    @Test
    public void testForId() throws Exception {
        Set<ClusterNode> nodesSet = new HashSet<>(Arrays.asList(newNode(), newNode(), newNode(), newNode()));

        for (ClusterNode n : nodesSet) {
            assertEquals(singleton(n), ClusterFilters.forNode(n.getId()).apply(nodesSet));
        }

        ClusterNodeId nonExisting = newNodeId();

        assertEquals(emptySet(), ClusterFilters.forNode(nonExisting).apply(nodesSet));
    }

    @Test
    public void testForNode() throws Exception {
        Set<ClusterNode> nodesSet = new HashSet<>(Arrays.asList(newNode(), newNode(), newNode(), newNode()));

        for (ClusterNode n : nodesSet) {
            assertEquals(singleton(n), ClusterFilters.forNode(n).apply(nodesSet));
        }

        ClusterNode nonExisting = newNode();

        assertEquals(singleton(nonExisting), ClusterFilters.forNode(nonExisting).apply(nodesSet));
    }

    @Test
    public void testForNext() throws Exception {
        repeat(5, i -> {
            List<ClusterNode> nodesList = new ArrayList<>();

            repeat(5, j -> nodesList.add(newNode(newNodeId(j), i == j /* <- Local node. */, null, null)));

            ClusterNode expected;

            if (i == 4) {
                // If last is local then should take the first one.
                expected = nodesList.get(0);
            } else {
                // Otherwise should take node that is next after the local.
                expected = nodesList.get(i + 1);
            }

            assertEquals(singleton(expected), ClusterFilters.forNext().apply(nodesList));
        });
    }

    @Test
    public void testForNextWithSingleLocal() throws Exception {
        ClusterNode node = newLocalNode();

        assertEquals(singleton(node), ClusterFilters.forNext().apply(singleton(node)));
    }

    @Test
    public void testForNextWithNoLocal() throws Exception {
        Set<ClusterNode> nodesSet = new HashSet<>(Arrays.asList(newNode(), newNode(), newNode(), newNode()));

        assertEquals(emptySet(), ClusterFilters.forNext().apply(nodesSet));
    }

    @Test
    public void testForJoinOrderNext() throws Exception {
        repeat(5, i -> {
            List<ClusterNode> nodesList = new ArrayList<>();

            repeat(5, j -> nodesList.add(newNode(newNodeId(), i == j /* <- Local node. */, null, null, j)));

            ClusterNode expected;

            if (i == 4) {
                // If last is local then should take the first one.
                expected = nodesList.get(0);
            } else {
                // Otherwise should take node that is next after the local.
                expected = nodesList.get(i + 1);
            }

            assertEquals(singleton(expected), ClusterFilters.forNextInJoinOrder().apply(nodesList));
        });
    }

    @Test
    public void testForJoinOrderNextWithSingleLocal() throws Exception {
        ClusterNode node = newLocalNode();

        assertEquals(singleton(node), ClusterFilters.forNextInJoinOrder().apply(singleton(node)));
    }

    @Test
    public void testForJoinOrderNextWithNoLocal() throws Exception {
        Set<ClusterNode> nodesSet = new HashSet<>(Arrays.asList(newNode(), newNode(), newNode(), newNode()));

        assertEquals(emptySet(), ClusterFilters.forNextInJoinOrder().apply(nodesSet));
    }

    @Test
    public void testForOldest() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        assertEquals(singleton(node1), ClusterFilters.forOldest().apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForYoungest() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        assertEquals(singleton(node3), ClusterFilters.forYoungest().apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForRole() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withRoles(singleton("role1")));
        ClusterNode node2 = newLocalNode(n -> n.withRoles(singleton("role2")));
        ClusterNode node3 = newLocalNode(n -> n.withRoles(singleton("role3")));

        assertEquals(singleton(node3), ClusterFilters.forRole("role3").apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forRole("INVALID").apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForProperty() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withProperties(singletonMap("prop1", "val1")));
        ClusterNode node2 = newLocalNode(n -> n.withProperties(singletonMap("prop2", "val2")));
        ClusterNode node3 = newLocalNode(n -> n.withProperties(singletonMap("prop3", "val3")));

        assertEquals(singleton(node3), ClusterFilters.forProperty("prop3").apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forProperty("INVALID").apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForPropertyValue() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withProperties(singletonMap("prop1", "val1")));
        ClusterNode node2 = newLocalNode(n -> n.withProperties(singletonMap("prop2", "val2")));
        ClusterNode node3 = newLocalNode(n -> n.withProperties(singletonMap("prop3", "val3")));

        assertEquals(singleton(node3), ClusterFilters.forProperty("prop3", "val3").apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forProperty("prop3", "INVALID").apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forProperty("INVALID", "INVALID").apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForService() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withServices(singletonMap(NetworkService.class.getName(), null)));
        ClusterNode node2 = newLocalNode(n -> n.withServices(singletonMap(ClusterService.class.getName(), null)));
        ClusterNode node3 = newLocalNode(n -> n.withServices(singletonMap(MessagingService.class.getName(), null)));

        assertEquals(singleton(node3), ClusterFilters.forService(MessagingService.class).apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forService(CoordinationService.class).apply(toSet(node1, node2, node3)));
    }

    @Test
    public void testForRemotes() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withLocalNode(false));
        ClusterNode node2 = newLocalNode(n -> n.withLocalNode(false));
        ClusterNode node3 = newLocalNode(n -> n.withLocalNode(true));

        assertEquals(toSet(node1, node2), ClusterFilters.forRemotes().apply(toSet(node1, node2, node3)));
        assertEquals(emptySet(), ClusterFilters.forService(CoordinationService.class).apply(toSet(node1, node2)));
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ClusterFilters.class);
    }
}
