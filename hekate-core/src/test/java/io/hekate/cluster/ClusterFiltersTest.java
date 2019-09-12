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
import io.hekate.coordinate.CoordinationService;
import io.hekate.messaging.MessagingService;
import io.hekate.network.NetworkService;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class ClusterFiltersTest extends HekateTestBase {
    @Test
    public void testForId() throws Exception {
        List<ClusterNode> nodes = asList(newNode(), newNode(), newNode(), newNode());

        for (ClusterNode n : nodes) {
            assertEquals(singletonList(n), ClusterFilters.forNode(n.id()).apply(nodes));
        }

        ClusterNodeId nonExisting = newNodeId();

        assertEquals(emptyList(), ClusterFilters.forNode(nonExisting).apply(nodes));
    }

    @Test
    public void testForNode() throws Exception {
        List<ClusterNode> nodes = asList(newNode(), newNode(), newNode(), newNode());

        for (ClusterNode n : nodes) {
            assertEquals(singletonList(n), ClusterFilters.forNode(n).apply(nodes));
        }

        ClusterNode nonExisting = newNode();

        assertEquals(emptyList(), ClusterFilters.forNode(nonExisting).apply(nodes));
    }

    @Test
    public void testForNext() throws Exception {
        repeat(5, i -> {
            List<ClusterNode> nodes = new ArrayList<>();

            repeat(5, j -> nodes.add(newNode(newNodeId(j), i == j /* <- Local node. */, null, null)));

            ClusterNode expected;

            if (i == 4) {
                // If last is local then should take the first one.
                expected = nodes.get(0);
            } else {
                // Otherwise should take node that is next after the local.
                expected = nodes.get(i + 1);
            }

            assertEquals(singletonList(expected), ClusterFilters.forNext().apply(nodes));
        });
    }

    @Test
    public void testForNextWithSingleLocal() throws Exception {
        ClusterNode node = newLocalNode();

        assertEquals(singletonList(node), ClusterFilters.forNext().apply(singletonList(node)));
    }

    @Test
    public void testForNextWithNoLocal() throws Exception {
        List<ClusterNode> nodes = asList(newNode(), newNode(), newNode(), newNode());

        assertEquals(emptyList(), ClusterFilters.forNext().apply(nodes));
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

            assertEquals(singletonList(expected), ClusterFilters.forNextInJoinOrder().apply(nodesList));
        });
    }

    @Test
    public void testForJoinOrderNextWithSingleLocal() throws Exception {
        ClusterNode node = newLocalNode();

        assertEquals(singletonList(node), ClusterFilters.forNextInJoinOrder().apply(singletonList(node)));
    }

    @Test
    public void testForJoinOrderNextWithNoLocal() throws Exception {
        List<ClusterNode> nodes = asList(newNode(), newNode(), newNode(), newNode());

        assertEquals(emptyList(), ClusterFilters.forNextInJoinOrder().apply(nodes));
    }

    @Test
    public void testForOldest() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        assertEquals(singletonList(node1), ClusterFilters.forOldest().apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForYoungest() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        assertEquals(singletonList(node3), ClusterFilters.forYoungest().apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForRole() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withRoles(singleton("role1")));
        ClusterNode node2 = newLocalNode(n -> n.withRoles(singleton("role2")));
        ClusterNode node3 = newLocalNode(n -> n.withRoles(singleton("role3")));

        assertEquals(singletonList(node3), ClusterFilters.forRole("role3").apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forRole("INVALID").apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForProperty() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withProperties(singletonMap("prop1", "val1")));
        ClusterNode node2 = newLocalNode(n -> n.withProperties(singletonMap("prop2", "val2")));
        ClusterNode node3 = newLocalNode(n -> n.withProperties(singletonMap("prop3", "val3")));

        assertEquals(singletonList(node3), ClusterFilters.forProperty("prop3").apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forProperty("INVALID").apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForPropertyValue() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withProperties(singletonMap("prop1", "val1")));
        ClusterNode node2 = newLocalNode(n -> n.withProperties(singletonMap("prop2", "val2")));
        ClusterNode node3 = newLocalNode(n -> n.withProperties(singletonMap("prop3", "val3")));

        assertEquals(singletonList(node3), ClusterFilters.forProperty("prop3", "val3").apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forProperty("prop3", "INVALID").apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forProperty("INVALID", "INVALID").apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForService() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withServices(singletonMap(NetworkService.class.getName(), null)));
        ClusterNode node2 = newLocalNode(n -> n.withServices(singletonMap(ClusterService.class.getName(), null)));
        ClusterNode node3 = newLocalNode(n -> n.withServices(singletonMap(MessagingService.class.getName(), null)));

        assertEquals(singletonList(node3), ClusterFilters.forService(MessagingService.class).apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forService(CoordinationService.class).apply(asList(node1, node2, node3)));
    }

    @Test
    public void testForRemotes() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withLocalNode(false));
        ClusterNode node2 = newLocalNode(n -> n.withLocalNode(false));
        ClusterNode node3 = newLocalNode(n -> n.withLocalNode(true));

        assertEquals(asList(node1, node2), ClusterFilters.forRemotes().apply(asList(node1, node2, node3)));
        assertEquals(emptyList(), ClusterFilters.forService(CoordinationService.class).apply(asList(node1, node2)));
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ClusterFilters.class);
    }
}
