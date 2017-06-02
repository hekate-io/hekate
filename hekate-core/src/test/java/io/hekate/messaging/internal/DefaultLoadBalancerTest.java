package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.core.HekateSupport;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.internal.DefaultFailoverContext;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.RendezvousHashMapper;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class DefaultLoadBalancerTest extends HekateTestBase {

    private ClusterNode n1;

    private ClusterNode n2;

    private ClusterNode n3;

    private LoadBalancer<Integer> balancer;

    private DefaultClusterTopology topology;

    private RendezvousHashMapper mapper;

    private HekateSupport hekate;

    @Before
    public void setUp() throws Exception {
        balancer = new DefaultLoadBalancer<>();

        hekate = mock(HekateSupport.class);

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
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, null, topology, hekate, mapper, Optional.empty());

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);

            allRoutes.add(route);
        }

        assertEquals(3, allRoutes.size());
    }

    @Test
    public void testNonAffinityWithFailure() throws Exception {
        FailoverContext failure = new DefaultFailoverContext(2, new Exception(), n1, toSet(n1, n2), FailoverRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {

            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, null, topology, hekate, mapper, Optional.of(failure));

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);
            assertEquals(route, n3.id());
        }
    }

    @Test
    public void testAffinity() throws Exception {
        Set<ClusterNodeId> allRoutes = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, hekate, mapper, Optional.empty());

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);

            allRoutes.add(route);
        }

        assertEquals(3, allRoutes.size());
    }

    @Test
    public void testAffinityWithFailure() throws Exception {
        PartitionMapper backupMapper = RendezvousHashMapper.of(topology).withBackupNodes(2).build();

        FailoverContext failure = new DefaultFailoverContext(2, new Exception(), n1, toSet(n1, n2), FailoverRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, hekate, backupMapper, Optional.of(failure));

            ClusterNodeId route = balancer.route(i, ctx);

            assertNotNull(route);
            assertEquals(route, n3.id());
        }
    }

    @Test
    public void testAffinityWithFailureNoBackupNodes() throws Exception {
        Set<ClusterNodeId> allRoutes = new HashSet<>();

        FailoverContext failure = new DefaultFailoverContext(2, new Exception(), n1, toSet(n1, n2), FailoverRoutingPolicy.RE_ROUTE);

        for (int i = 0; i < 100; i++) {
            DefaultLoadBalancerContext ctx = new DefaultLoadBalancerContext(i, i, topology, hekate, mapper, Optional.of(failure));

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
