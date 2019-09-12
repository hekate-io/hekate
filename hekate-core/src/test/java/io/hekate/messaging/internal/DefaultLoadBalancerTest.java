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

package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.messaging.loadbalance.DefaultLoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultLoadBalancerTest extends HekateTestBase {
    private ClusterNode n1;

    private ClusterNode n2;

    private ClusterNode n3;

    private LoadBalancer<Integer> balancer;

    private DefaultClusterTopology topology;

    private RendezvousHashMapper mapper;

    @Before
    public void setUp() throws Exception {
        balancer = new DefaultLoadBalancer<>();

        n1 = newNode();
        n2 = newNode();
        n3 = newNode();

        topology = DefaultClusterTopology.of(1, toSet(n1, n2, n3));

        mapper = RendezvousHashMapper.of(topology).build();
    }

    @Test
    public void testNonAffinity() throws Exception {
        Set<ClusterNodeId> allRoutes = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, null, topology, mapper, Optional.empty());

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);

            allRoutes.add(route);
        }

        assertEquals(3, allRoutes.size());
    }

    @Test
    public void testNonAffinityWithFailure() throws Exception {
        FailedAttempt failure = new MessageOperationFailure(2, new Exception(), n1, toSet(n1, n2), RetryRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {

            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, null, topology, mapper, Optional.of(failure));

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);
            assertEquals(route, n3.id());
        }
    }

    @Test
    public void testAffinity() throws Exception {
        Set<ClusterNodeId> allRoutes = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, mapper, Optional.empty());

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);

            allRoutes.add(route);
        }

        assertEquals(3, allRoutes.size());
    }

    @Test
    public void testAffinityWithFailure() throws Exception {
        PartitionMapper backupMapper = RendezvousHashMapper.of(topology).withBackupNodes(2).build();

        FailedAttempt failure = new MessageOperationFailure(2, new Exception(), n1, toSet(n1, n2), RetryRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, backupMapper, Optional.of(failure));

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);
            assertEquals(route, n3.id());
        }
    }

    @Test
    public void testAffinityWithFailureNoBackupNodes() throws Exception {
        Set<ClusterNodeId> allRoutes = new HashSet<>();

        FailedAttempt failure = new MessageOperationFailure(2, new Exception(), n1, toSet(n1, n2), RetryRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, mapper, Optional.of(failure));

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);

            allRoutes.add(route);
        }

        assertEquals(3, allRoutes.size());
    }

    @Test
    public void testToString() {
        assertEquals(DefaultLoadBalancer.class.getSimpleName(), balancer.toString());
    }
}
