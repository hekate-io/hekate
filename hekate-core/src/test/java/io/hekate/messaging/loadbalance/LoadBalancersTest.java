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

package io.hekate.messaging.loadbalance;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.messaging.internal.LoadBalancerContextMock;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class LoadBalancersTest extends HekateTestBase {
    private LoadBalancerContext ctx;

    @Before
    public void setUp() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        ctx = newContext(toSet(node1, node2, node3));
    }

    @Test
    public void testRandom() throws Exception {
        LoadBalancer<Object> lb = LoadBalancers.random();

        ClusterNodeId nodeId = lb.route("msg", ctx);

        assertNotNull(nodeId);
        assertTrue(ctx.contains(nodeId));

        assertEquals("Random", lb.toString());
    }

    @Test
    public void testRoundRobin() throws Exception {
        LoadBalancer<Object> lb = LoadBalancers.newRoundRobin();

        int idx = 0;

        for (int i = 0; i < 100; i++) {
            ClusterNodeId nodeId = lb.route("msg", ctx);

            assertNotNull(nodeId);
            assertTrue(ctx.contains(nodeId));

            assertEquals(ctx.nodes().get(idx).id(), nodeId);

            if (idx == ctx.size() - 1) {
                idx = 0;
            } else {
                idx++;
            }
        }

        assertEquals("RoundRobin", lb.toString());

        // Check that always returns a new instance.
        assertNotSame(lb, LoadBalancers.newRoundRobin());
        assertNotSame(lb, LoadBalancers.newRoundRobin());
        assertNotSame(lb, LoadBalancers.newRoundRobin());

        runParallel(4, 1000, i ->
            assertNotNull(lb.route(i, ctx))
        );
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(LoadBalancers.class);
    }

    private LoadBalancerContext newContext(Set<ClusterNode> nodesSet) {
        return new LoadBalancerContextMock(100, null, DefaultClusterTopology.of(1, nodesSet));
    }
}
