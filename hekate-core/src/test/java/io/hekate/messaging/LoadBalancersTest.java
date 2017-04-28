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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterUuid;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.messaging.internal.LoadBalancerContextBridge;
import io.hekate.messaging.unicast.LoadBalancerContext;
import io.hekate.messaging.unicast.LoadBalancers;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LoadBalancersTest extends HekateTestBase {
    @Test
    public void testToRandom() throws Exception {
        ClusterNode node1 = newLocalNode(n -> n.withJoinOrder(1));
        ClusterNode node2 = newLocalNode(n -> n.withJoinOrder(2));
        ClusterNode node3 = newLocalNode(n -> n.withJoinOrder(3));

        LoadBalancerContext ctx = newContext(toSet(node1, node2, node3));

        ClusterUuid nodeId = LoadBalancers.toRandom().route("msg", ctx);

        assertNotNull(nodeId);
        assertTrue(ctx.contains(nodeId));
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(LoadBalancers.class);
    }

    private LoadBalancerContext newContext(Set<ClusterNode> nodesSet) {
        return new LoadBalancerContextBridge(100, null, DefaultClusterTopology.of(1, nodesSet));
    }
}
