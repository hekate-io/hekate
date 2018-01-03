/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopologyTestBase;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.core.HekateSupport;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DefaultLoadBalancerContextTest extends ClusterTopologyTestBase {
    @Test
    public void testFailoverDetails() throws Exception {
        FailureInfo details = mock(FailureInfo.class);

        ClusterNode n1 = newNode();

        LoadBalancerContext ctx = newContext(100, "test", 1, toSet(n1, newNode()), details);

        assertNotNull(ctx.failure());
        assertEquals(100, ctx.affinity());
        assertEquals("test", ctx.affinityKey());
        assertTrue(ctx.hasAffinity());
        assertSame(details, ctx.failure().get());
        assertSame(details, ctx.filter(n -> false).failure().get());
        assertSame(details, ctx.filter(n -> n.equals(n1)).failure().get());
    }

    @Override
    protected DefaultLoadBalancerContext newTopology(int version, Set<ClusterNode> nodes) {
        return newContext(100, "test", version, nodes, null);
    }

    private DefaultLoadBalancerContext newContext(int affinity, Object affinityKey, int ver, Set<ClusterNode> nodes, FailureInfo failure) {
        DefaultClusterTopology topology = DefaultClusterTopology.of(ver, nodes);
        Optional<FailureInfo> optFailure = Optional.ofNullable(failure);
        PartitionMapper partitions = mock(PartitionMapper.class);
        HekateSupport hekate = mock(HekateSupport.class);

        return new DefaultLoadBalancerContext(affinity, affinityKey, topology, hekate, partitions, optFailure);
    }
}
