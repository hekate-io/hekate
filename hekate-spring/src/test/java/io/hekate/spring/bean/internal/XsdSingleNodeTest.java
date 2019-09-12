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

package io.hekate.spring.bean.internal;

import foo.bar.SomeClusterAcceptor;
import foo.bar.SomeRpcService;
import foo.bar.SomeRpcServiceImpl;
import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProvider;
import io.hekate.cluster.split.HostReachabilityDetector;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetectorGroup;
import io.hekate.codec.CodecService;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.jmx.JmxService;
import io.hekate.election.ElectionService;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingService;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkService;
import io.hekate.rpc.RpcClientBuilder;
import io.hekate.rpc.RpcServerInfo;
import io.hekate.rpc.RpcService;
import io.hekate.util.format.ToString;
import java.lang.management.ManagementFactory;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath*:xsd-test/test-single-node.xml")
public class XsdSingleNodeTest extends HekateTestBase {
    @Autowired
    private HekateBootstrap bootstrap;

    @Autowired
    @Qualifier("hekateBean")
    private Hekate hekate;

    @Autowired
    @Qualifier("cluster")
    private ClusterService cluster;

    @Autowired
    @Qualifier("locks")
    private LockService locks;

    @Autowired
    @Qualifier("messaging")
    private MessagingService messaging;

    @Autowired
    @Qualifier("rpc")
    private RpcService rpc;

    @Autowired
    @Qualifier("someRpcClient1")
    private SomeRpcService rpcClient;

    @Autowired
    @Qualifier("coordination")
    private CoordinationService coordination;

    @Autowired
    @Qualifier("election")
    private ElectionService election;

    @Autowired
    @Qualifier("network")
    private NetworkService network;

    @Autowired
    @Qualifier("test.protocol.1")
    private NetworkConnector<Object> networkConnector;

    @Autowired
    @Qualifier("some.channel")
    private MessagingChannel<Object> messagingChannel;

    @Autowired
    @Qualifier("region1")
    private LockRegion lockRegion;

    @Autowired
    @Qualifier("someLock1")
    private DistributedLock lock;

    @Test
    public void test() {
        assertNotNull(bootstrap);
        assertNotNull(hekate);
        assertNotNull(cluster);
        assertNotNull(locks);
        assertNotNull(messaging);
        assertNotNull(rpc);
        assertNotNull(rpcClient);
        assertNotNull(coordination);
        assertNotNull(election);
        assertNotNull(network);
        assertNotNull(networkConnector);
        assertNotNull(messagingChannel);
        assertNotNull(lockRegion);
        assertNotNull(lock);

        verifyJmx();

        verifyLocalNode();

        verifyCluster();

        verifyMessaging();

        verifyRpc();

        verifyCoordination();

        say("Done: " + ToString.format(this));
    }

    @Override
    protected void checkGhostThreads() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }

    private void verifyJmx() {
        assertTrue(hekate.has(JmxService.class));

        JmxService jmx = hekate.get(JmxService.class);

        assertNotNull(jmx);
        assertEquals("foo.bar", jmx.domain());
        assertSame(ManagementFactory.getPlatformMBeanServer(), jmx.server());
    }

    private void verifyLocalNode() {
        assertEquals("test-cluster", bootstrap.getClusterName());
        assertEquals("test-node", hekate.localNode().name());

        assertEquals(3, hekate.localNode().roles().size());
        assertEquals(toSet("role 1", "role 2", "role 3"), hekate.localNode().roles());

        assertEquals("prop val 1", hekate.localNode().property("prop 1"));
        assertEquals("prop val 2", hekate.localNode().property("prop 2"));
        assertEquals("prop val 3", hekate.localNode().property("prop 3"));

        assertEquals("some-value-from-provider", hekate.localNode().property("some-property-from-provider"));

        assertTrue(hekate.get(CodecService.class).codecFactory().toString().contains(FstCodecFactory.class.getSimpleName()));
    }

    private void verifyMessaging() {
        assertNotNull(hekate.messaging().channel("some.channel"));
        assertNotNull(hekate.messaging().channel("another.channel"));

        assertEquals(String.class, hekate.messaging().channel("some.channel").baseType());
    }

    private void verifyCluster() {
        DefaultClusterService cluster = hekate.get(DefaultClusterService.class);

        JdbcSeedNodeProvider jdbc = (JdbcSeedNodeProvider)cluster.seedNodeProvider();

        assertEquals(60001, jdbc.cleanupInterval());
        assertEquals(5, jdbc.queryTimeout());
        assertEquals(JdbcDataSource.class, jdbc.dataSource().getClass());
        assertTrue(jdbc.insertSql(), jdbc.insertSql().startsWith("INSERT INTO cn (h, p, c)"));

        DefaultFailureDetector detector = (DefaultFailureDetector)cluster.failureDetector();

        assertEquals(501, detector.heartbeatInterval());
        assertEquals(5, detector.heartbeatLossThreshold());
        assertEquals(3, detector.failureQuorum());

        assertSame(SplitBrainAction.TERMINATE, cluster.splitBrainAction());

        SplitBrainDetectorGroup splitBrain = (SplitBrainDetectorGroup)cluster.splitBrainDetector();

        assertSame(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID, splitBrain.getGroupPolicy());
        assertEquals(2, splitBrain.getDetectors().size());

        assertEquals(HostReachabilityDetector.class, splitBrain.getDetectors().get(0).getClass());
        assertEquals(SplitBrainDetectorGroup.class, splitBrain.getDetectors().get(1).getClass());

        assertEquals(1, cluster.acceptors().stream().filter(a -> a instanceof SomeClusterAcceptor).count());
    }

    private void verifyRpc() {
        assertEquals(2, hekate.rpc().servers().size());

        RpcServerInfo server1 = hekate.rpc().servers().stream()
            .filter(s -> s.tags().contains("server-1-tag-1"))
            .findFirst()
            .orElseThrow(AssertionError::new);

        RpcServerInfo server2 = hekate.rpc().servers().stream()
            .filter(s -> s.tags().contains("server-2-tag-1"))
            .findFirst()
            .orElseThrow(AssertionError::new);

        RpcClientBuilder<SomeRpcService> client1 = hekate.rpc().clientFor(SomeRpcService.class, "some-tag-1");
        RpcClientBuilder<SomeRpcService> client2 = hekate.rpc().clientFor(SomeRpcService.class, "some-tag-2");

        assertEquals(SomeRpcServiceImpl.class, server1.rpc().getClass());
        assertEquals(toSet("server-1-tag-1", "server-1-tag-2"), server1.tags());

        assertEquals(SomeRpcServiceImpl.class, server2.rpc().getClass());
        assertEquals(toSet("server-2-tag-1", "server-2-tag-2"), server2.tags());
    }

    private void verifyCoordination() {
        assertNotNull(hekate.coordination().process("someProcess1"));
        assertNotNull(hekate.coordination().process("someProcess2"));
    }
}
