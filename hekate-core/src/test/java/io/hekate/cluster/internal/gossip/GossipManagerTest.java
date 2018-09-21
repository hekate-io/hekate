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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.health.FailureDetectorMock;
import io.hekate.cluster.internal.DefaultClusterNodeBuilder;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.Update;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateBase;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GossipManagerTest extends HekateTestBase {
    private static final String CLUSTER_ID = "test";

    private FailureDetectorMock.GlobalNodesState nodeFailures;

    @Before
    public void setUp() {
        nodeFailures = new FailureDetectorMock.GlobalNodesState();
    }

    @Test
    public void testRejectJoinRequestIfNotJoined() throws Exception {
        GossipManager m = createManager(1);

        InetSocketAddress address = m.address().socket();

        assertFalse(m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, address)).isAccept());
    }

    @Test
    public void testRejectJoinRequestFromAnotherCluster() throws Exception {
        GossipManager m = createManager(1);

        InetSocketAddress address = m.address().socket();

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID + "_another", address));

        assertNotNull(reply);
        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.PERMANENT, reply.asReject().rejectType());
    }

    @Test
    public void testDoNotJoinIfJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.status());

        assertNull(m.join(Collections.emptyList()));
        assertNull(m.join(Collections.singletonList(newSocketAddress())));
    }

    @Test
    public void testSelfJoinAfterTryMultipleSeedNodes() throws Exception {
        GossipManager m = createManager(1);

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        seedNodes.add(newSocketAddress(2));
        seedNodes.add(newSocketAddress(3));
        seedNodes.add(newSocketAddress(4));

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.status());

        for (InetSocketAddress addr : seedNodes) {
            m.processJoinReject(JoinReject.retryLater(newAddress(addr), m.address(), addr));
        }

        m.join(seedNodes);

        assertSame(GossipNodeStatus.UP, m.status());
    }

    @Test
    public void testNoSelfJoinAfterTryMultipleSeedNodes() throws Exception {
        GossipManager m = createManager(10000);

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        seedNodes.add(newSocketAddress(2));
        seedNodes.add(newSocketAddress(3));
        seedNodes.add(newSocketAddress(4));

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.status());

        for (InetSocketAddress addr : seedNodes) {
            m.processJoinReject(JoinReject.retryLater(newAddress(addr), m.address(), addr));
        }

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.status());
    }

    @Test
    public void testJoinRetryOfFailedSeedNode() throws Exception {
        GossipManager m = createManager(10000);

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        seedNodes.add(newSocketAddress(2));
        seedNodes.add(newSocketAddress(3));
        seedNodes.add(newSocketAddress(4));

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.status());

        InetSocketAddress failed = seedNodes.remove(0);

        JoinRequest request = m.processJoinFailure(new JoinRequest(m.node(), CLUSTER_ID, failed), TEST_ERROR);

        assertEquals(seedNodes.toString(), failed, request.toAddress());

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        seedNodes.add(0, failed);

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).toAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.status());
    }

    @Test
    public void testDoNotProcessJoinFailureIfJoined() throws Exception {
        GossipManager m = createManager(1);

        InetSocketAddress failed = newSocketAddress(2);

        assertEquals(failed, m.join(Collections.singletonList(failed)).toAddress());

        assertEquals(failed, m.processJoinFailure(new JoinRequest(m.node(), CLUSTER_ID, failed), TEST_ERROR).toAddress());

        assertNull(m.join(Collections.singletonList(failed)));

        assertSame(GossipNodeStatus.UP, m.status());

        assertNull(m.processJoinFailure(new JoinRequest(m.node(), CLUSTER_ID, failed), TEST_ERROR));
    }

    @Test
    public void testDoNotProcessJoinAcceptIfJoined() throws Exception {
        GossipManager fake = createManager(2);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.status());

        assertNull(m.processJoinAccept(new JoinAccept(fake.address(), m.address(), fake.localGossip())));

        assertSame(GossipNodeStatus.UP, m.status());

        assertEquals(1, m.localGossip().members().size());
        assertTrue(m.localGossip().members().containsKey(m.id()));
    }

    @Test
    public void testDoNotProcessJoinRejectIfJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.status());

        assertNull(m.processJoinReject(JoinReject.retryLater(newAddress(1), m.address(), newSocketAddress())));

        assertSame(GossipNodeStatus.UP, m.status());

        assertEquals(1, m.localGossip().members().size());
        assertTrue(m.localGossip().members().containsKey(m.id()));
    }

    @Test
    public void testDoNotProcessJoinRequestIfNotJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.singletonList(newSocketAddress(2)));

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertFalse(m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.address().socket())).isAccept());

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertTrue(m.localGossip().members().isEmpty());
    }

    @Test
    public void testDoNotProcessJoinRequestFromSelf() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.singletonList(newSocketAddress(2)));

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertFalse(m.processJoinRequest(new JoinRequest(m.node(), CLUSTER_ID, m.address().socket())).isAccept());

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertTrue(m.localGossip().members().isEmpty());
    }

    @Test
    public void testDoNotProcessJoinRequestIfDownAfterLeave() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.status());

        m.leave();

        assertSame(GossipNodeStatus.DOWN, m.status());

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.address().socket()));

        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.TEMPORARY, reply.asReject().rejectType());
    }

    @Test
    public void testDoNotProcessJoinRequestFromOtherCluster() throws Exception {
        GossipManager m = createManager(1, CLUSTER_ID + "-other");

        m.join(Collections.emptyList());

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.address().socket()));

        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.PERMANENT, reply.asReject().rejectType());
    }

    @Test
    public void testDoNotProcessJoinRequestIfJoiningNodeFails() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        ClusterNode newNode = newNode();

        // First attempt to join (should succeed).
        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode, CLUSTER_ID, m.address().socket()));

        assertTrue(reply.isAccept());

        // Joining node is assumed to be failed.
        nodeFailures.markFailed(m.id().id(), newNode.id());

        assertTrue(m.checkAliveness());

        assertTrue(m.localGossip().isDown(newNode.id()));

        // First attempt to join (should fail).
        JoinReply reply2 = m.processJoinRequest(new JoinRequest(newNode, CLUSTER_ID, m.address().socket()));

        assertFalse(reply2.isAccept());
        assertSame(JoinReject.RejectType.CONFLICT, reply2.asReject().rejectType());
    }

    @Test
    public void testDoNotProcessJoinAcceptIfNotMember() throws Exception {
        GossipManager fake = createManager(2);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(1);

        m.join(Collections.singletonList(fake.address().socket()));

        assertNull(m.processJoinAccept(new JoinAccept(fake.address(), m.address(), fake.localGossip())));

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertTrue(m.localGossip().members().isEmpty());
    }

    @Test
    public void testDoNotProcessUpdateIfDown() throws Exception {
        GossipManager fake = createManager(1);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m.status());

        assertNull(m.processUpdate(new Update(fake.address(), m.address(), fake.localGossip())));

        assertSame(GossipNodeStatus.DOWN, m.status());
        assertTrue(m.localGossip().members().isEmpty());
    }

    @Test
    public void testDoNotProcessUpdateIfNotInRemoteView() throws Exception {
        GossipManager fake = createManager(1);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(2);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.status());

        assertNull(m.processUpdate(new Update(fake.address(), m.address(), fake.localGossip())));

        assertSame(GossipNodeStatus.UP, m.status());
        assertEquals(1, m.localGossip().members().size());
        assertTrue(m.localGossip().members().containsKey(m.id()));
    }

    @Test
    public void testJoinTwoNodes() throws Exception {
        GossipManager m1 = createManager(1);
        GossipManager m2 = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m1.status());
        assertSame(GossipNodeStatus.DOWN, m1.status());

        assertNull(m1.join(Collections.emptyList()));

        assertSame(GossipNodeStatus.UP, m1.status());

        JoinRequest join = m2.join(Collections.singletonList(m1.address().socket()));

        assertNotNull(join);
        assertEquals(m2.address(), join.from());
        assertEquals(m1.address().socket(), join.toAddress());

        say("Hey bro! Welcome!");

        JoinAccept rep = m1.processJoinRequest(join).asAccept();

        assertNotNull(rep);
        assertEquals(m1.address(), rep.from());
        assertEquals(m2.address(), rep.to());
        assertEquals(2, rep.gossip().members().size());
        assertTrue(rep.gossip().members().containsKey(m1.id()));
        assertTrue(rep.gossip().members().containsKey(m2.id()));
        assertEquals(1, rep.gossip().seen().size());
        assertTrue(rep.gossip().seen().contains(m1.id()));

        say("Tnx bro! I'll confirm that I've seen it.");

        Update gos = m2.processJoinAccept(rep);

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertEquals(2, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));
        assertTrue(gos.gossip().seen().contains(m2.id()));
        assertSame(GossipNodeStatus.JOINING, m2.status());

        say("...o-o-kay, looks like we have the same view. I'll put you to UP state.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        say("Ok, I'm UP, cool! Now I'll confirm that I've seen it.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertEquals(2, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));
        assertTrue(gos.gossip().seen().contains(m2.id()));
        assertSame(GossipNodeStatus.UP, m2.status());

        say("We are done.");

        assertNull(m1.processUpdate(gos));

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());
    }

    @Test
    public void testJoinThreeNodes() throws Exception {
        List<GossipManager> nodes = startNodes(3);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);
        GossipManager m3 = nodes.get(2);

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());
        assertSame(GossipNodeStatus.UP, m3.status());

        assertTrue(m1.localGossip().isConvergent());
        assertTrue(m2.localGossip().isConvergent());
        assertTrue(m3.localGossip().isConvergent());

        Gossip g1 = m1.localGossip();

        say("Gossip " + m1.node() + ": " + g1);

        assertEquals(3, g1.members().size());
        assertSame(GossipNodeStatus.UP, g1.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g1.member(m2.id()).status());
        assertSame(GossipNodeStatus.UP, g1.member(m3.id()).status());
        assertEquals(0, g1.suspectedView().suspected().size());
        assertSame(ComparisonResult.SAME, g1.compare(m2.localGossip()));
        assertSame(ComparisonResult.SAME, g1.compare(m3.localGossip()));
        assertEquals(3, g1.seen().size());
        assertTrue(g1.seen().contains(m1.id()));
        assertTrue(g1.seen().contains(m2.id()));
        assertTrue(g1.seen().contains(m3.id()));

        Gossip g2 = m2.localGossip();

        say("Gossip " + m2.node() + ": " + g2);

        assertEquals(3, g2.members().size());
        assertSame(GossipNodeStatus.UP, g2.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g2.member(m2.id()).status());
        assertSame(GossipNodeStatus.UP, g2.member(m3.id()).status());
        assertEquals(0, g2.suspectedView().suspected().size());
        assertSame(ComparisonResult.SAME, g2.compare(m1.localGossip()));
        assertSame(ComparisonResult.SAME, g2.compare(m3.localGossip()));
        assertEquals(3, g2.seen().size());
        assertTrue(g2.seen().contains(m1.id()));
        assertTrue(g2.seen().contains(m2.id()));
        assertTrue(g2.seen().contains(m3.id()));

        Gossip g3 = m3.localGossip();

        say("Gossip " + m3.node() + ": " + g3);

        assertEquals(3, g3.members().size());
        assertSame(GossipNodeStatus.UP, g3.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m2.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m3.id()).status());
        assertEquals(0, g3.suspectedView().suspected().size());
        assertSame(ComparisonResult.SAME, g3.compare(m1.localGossip()));
        assertSame(ComparisonResult.SAME, g3.compare(m2.localGossip()));
        assertEquals(3, g3.seen().size());
        assertTrue(g3.seen().contains(m1.id()));
        assertTrue(g3.seen().contains(m2.id()));
        assertTrue(g3.seen().contains(m3.id()));
    }

    @Test
    public void testLeave() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());

        m2.leave();

        say("Sorry Bro, I have to go...");

        Update gos = m2.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m2.id()));

        say("Well, ok, let me set you to DOWN state.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        say("Ok, I'm in DOWN state.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m2.id()).status());
        assertEquals(2, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));
        assertTrue(gos.gossip().seen().contains(m2.id()));

        say("Before going, I'll confirm that I've seen it.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(1, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.DOWN, m2.status());

        m2.leave();

        assertSame(GossipNodeStatus.DOWN, m2.status());

        UpdateBase dig = m2.gossip();

        assertNotNull(dig);
        assertEquals(m2.address(), dig.from());
        assertEquals(m1.address(), dig.to());
        assertEquals(2, dig.gossipBase().membersInfo().size());
        assertTrue(dig.gossipBase().membersInfo().containsKey(m1.id()));
        assertTrue(dig.gossipBase().membersInfo().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, dig.gossipBase().membersInfo().get(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, dig.gossipBase().membersInfo().get(m2.id()).status());
    }

    @Test
    public void testLeaveSameTime() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());

        m1.leave();
        m2.leave();

        say("Sorry Bro, I have to go...");

        Update gos = m2.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m2.id()));

        assertSame(GossipNodeStatus.LEAVING, m2.status());

        say("Oh! Me too!");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        assertSame(GossipNodeStatus.LEAVING, m1.status());

        say("Well, ok, now I know that you are leaving too.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.LEAVING, gos.gossip().members().get(m2.id()).status());
        assertEquals(2, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));
        assertTrue(gos.gossip().seen().contains(m2.id()));

        say("Bye then.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        assertSame(GossipNodeStatus.DOWN, m1.status());

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.address(), gos.from());
        assertEquals(m1.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, gos.gossip().members().get(m2.id()).status());
        assertEquals(2, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));
        assertTrue(gos.gossip().seen().contains(m2.id()));

        assertSame(GossipNodeStatus.DOWN, m2.status());
    }

    @Test
    public void testFailWithTwoNodes() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());

        nodeFailures.markFailed(m1.id(), m2.id());

        assertTrue(m1.checkAliveness());

        // Should detect failures only once.
        assertFalse(m1.checkAliveness());

        UpdateBase gos = m1.gossip();

        assertNull(gos);
    }

    @Test
    public void testFailWithThreeNodes() throws Exception {
        List<GossipManager> nodes = startNodes(3);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);
        GossipManager m3 = nodes.get(2);

        assertSame(GossipNodeStatus.UP, m1.status());
        assertSame(GossipNodeStatus.UP, m2.status());
        assertSame(GossipNodeStatus.UP, m3.status());

        say("...I'm dying ...alone ...in the darkness!!!");

        nodeFailures.markFailed(m1.id(), m2.id());
        nodeFailures.markFailed(m3.id(), m2.id());

        say("Lets see how is he doing...");

        assertTrue(m1.checkAliveness());
        assertTrue(m3.checkAliveness());

        // Should detect failures only once.
        assertFalse(m1.checkAliveness());
        assertFalse(m3.checkAliveness());

        Gossip g1 = m1.localGossip();

        assertEquals(3, g1.members().size());
        assertSame(GossipNodeStatus.UP, g1.member(m1.id()).status());
        assertSame(GossipNodeStatus.DOWN, g1.member(m2.id()).status());
        assertSame(GossipNodeStatus.UP, g1.member(m3.id()).status());
        assertEquals(1, g1.suspectedView().suspected().size());
        assertTrue(g1.suspectedView().suspecting(m2.id()).contains(m1.id()));

        Gossip g3 = m3.localGossip();

        assertEquals(3, g3.members().size());
        assertSame(GossipNodeStatus.UP, g3.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m2.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m3.id()).status());
        assertEquals(1, g3.suspectedView().suspected().size());
        assertTrue(g3.suspectedView().suspecting(m2.id()).contains(m3.id()));

        say("OMG! He is dead! ...lets get rid of his body.");

        gossipTillSameVersion(m1, m3);

        g1 = m1.localGossip();

        assertEquals(2, g1.members().size());
        assertSame(GossipNodeStatus.UP, g1.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g1.member(m3.id()).status());
        assertEquals(1, g1.suspectedView().suspected().size());
        assertFalse(g1.suspectedView().suspecting(m2.id()).contains(m1.id()));
        assertTrue(g1.suspectedView().suspecting(m2.id()).contains(m3.id()));

        g3 = m3.localGossip();

        assertEquals(2, g3.members().size());
        assertSame(GossipNodeStatus.UP, g3.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m3.id()).status());
        assertEquals(1, g3.suspectedView().suspected().size());
        assertFalse(g3.suspectedView().suspecting(m2.id()).contains(m1.id()));
        assertTrue(g3.suspectedView().suspecting(m2.id()).contains(m3.id()));

        say("Hm, one more thing. We need to remove evidences of his existence.");

        assertFalse(m1.checkAliveness());

        assertTrue(m3.checkAliveness());

        // Should detect failures only once.
        assertFalse(m3.checkAliveness());

        gossipTillSameVersion(m1, m3);

        g1 = m1.localGossip();

        assertEquals(2, g1.members().size());
        assertSame(GossipNodeStatus.UP, g1.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g1.member(m3.id()).status());
        assertEquals(0, g1.suspectedView().suspected().size());

        g3 = m3.localGossip();

        assertEquals(2, g3.members().size());
        assertSame(GossipNodeStatus.UP, g3.member(m1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(m3.id()).status());
        assertEquals(0, g3.suspectedView().suspected().size());
    }

    @Test
    public void testFailOnJoin() throws Exception {
        GossipManager m1 = createManager(1);
        GossipManager m2 = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m1.status());
        assertSame(GossipNodeStatus.DOWN, m1.status());

        assertNull(m1.join(Collections.emptyList()));

        assertSame(GossipNodeStatus.UP, m1.status());

        say("Hey bro! Let me join!");

        JoinRequest join = m2.join(Collections.singletonList(m1.address().socket()));

        assertNotNull(join);
        assertEquals(m2.address(), join.from());
        assertEquals(m1.address().socket(), join.toAddress());

        m1.processJoinRequest(join);

        say("Whoops! I failed.");

        nodeFailures.markFailed(m1.id(), m2.id());

        Update gos = m1.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m1.address(), gos.from());
        assertEquals(m2.address(), gos.to());
        assertEquals(2, gos.gossip().members().size());
        assertTrue(gos.gossip().members().containsKey(m1.id()));
        assertTrue(gos.gossip().members().containsKey(m2.id()));
        assertSame(GossipNodeStatus.UP, gos.gossip().members().get(m1.id()).status());
        assertSame(GossipNodeStatus.JOINING, gos.gossip().members().get(m2.id()).status());
        assertEquals(1, gos.gossip().seen().size());
        assertTrue(gos.gossip().seen().contains(m1.id()));

        m1.checkAliveness();

        assertNull(m1.gossip());

        Gossip m1Gos = m1.localGossip();

        assertEquals(1, m1Gos.members().size());
        assertTrue(m1Gos.members().containsKey(m1.id()));
        assertSame(GossipNodeStatus.UP, m1Gos.members().get(m1.id()).status());
        assertEquals(1, m1Gos.seen().size());
        assertTrue(m1Gos.seen().contains(m1.id()));
    }

    @Test
    public void testUpdateDigest() throws Exception {
        List<GossipManager> nodes = startNodes(3);

        Map<ClusterAddress, GossipManager> nodesById = new HashMap<>();

        nodes.forEach(n -> nodesById.put(n.address(), n));

        nodes.forEach(n -> assertSame(GossipNodeStatus.UP, n.status()));

        nodes.forEach(n -> {
            UpdateBase msg = n.gossip();

            // Check that it is a gossip digest.
            assertNull(msg.asUpdate());

            UpdateBase rsp = nodesById.get(msg.to()).processUpdate(msg);

            // Should not reply if has the same gossip version.
            assertNull(rsp);
        });

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        m1.leave();

        while (true) {
            UpdateBase g1 = m2.gossip();

            if (g1.to().equals(m1.address())) {
                // Must be digest.
                assertNull(g1.asUpdate());

                UpdateBase g2 = m1.processUpdate(g1);

                // m1 has later version -> must reply with gossip update.
                assertEquals(m2.address(), g2.to());
                assertNotNull(g2.asUpdate());

                UpdateBase g3 = m2.processUpdate(g2);

                // Must send a gossip update to update seen list.
                assertSame(m1.address(), g3.to());
                assertNotNull(g3.asUpdate());

                UpdateBase g4 = m1.processUpdate(g3);

                // Must not gossip since speed-up gossipping is disabled.
                assertNull(g4);

                break;
            }
        }
    }

    private List<GossipManager> startNodes(int size) throws Exception {
        List<GossipManager> nodes = new ArrayList<>();

        GossipManager first = null;

        for (int i = 0; i < size; i++) {
            GossipManager mgr = createManager(i + 1);

            nodes.add(mgr);

            if (first == null) {
                first = mgr;

                mgr.join(Collections.emptyList());
            } else {
                JoinRequest join = mgr.join(Collections.singletonList(first.address().socket()));

                JoinAccept reply = first.processJoinRequest(join).asAccept();

                Update gossip = mgr.processJoinAccept(reply);

                processGossipConversation(first, mgr, gossip);
            }
        }

        gossipTillConvergence(nodes);

        nodes.sort(Comparator.comparing(GossipManager::node));

        say("Started: " + nodes.stream().map(GossipManager::node).collect(Collectors.toList()));

        return nodes;
    }

    private void gossipTillSameVersion(GossipManager... nodes) {
        assertNotNull(nodes);

        gossipTillSameVersion(Arrays.asList(nodes));
    }

    private void gossipTillSameVersion(List<GossipManager> nodes) {
        assertTrue(nodes.size() > 1);

        Map<ClusterNodeId, GossipManager> nodesById = nodes.stream().collect(Collectors.toMap(GossipManager::id, m -> m));

        int i = 0;

        do {
            say("Round: " + i);

            i++;

            int j = 0;

            for (GossipManager mgr : nodes) {
                say("Iteration: " + i + "-" + j++);

                UpdateBase gossip = mgr.gossip();

                GossipManager to = nodesById.get(gossip.to().id());

                assertNotNull("Unexpected gossip target.", to);

                processGossipConversation(to, mgr, gossip);
            }
        } while (!isSameGossipVersion(nodes));
    }

    private void gossipTillConvergence(List<GossipManager> nodes) {
        while (!isConvergence(nodes)) {
            gossipTillSameVersion(nodes);
        }
    }

    private boolean isConvergence(List<GossipManager> nodes) {
        if (!isSameGossipVersion(nodes)) {
            return false;
        }

        for (GossipManager m : nodes) {
            if (!m.localGossip().isConvergent()) {
                return false;
            }
        }

        return true;
    }

    private void processGossipConversation(GossipManager m1, GossipManager m2, UpdateBase gossip) {
        while (gossip != null) {
            if (gossip.to().equals(m1.address())) {
                gossip = m1.processUpdate(gossip);
            } else {
                gossip = m2.processUpdate(gossip);
            }
        }
    }

    private boolean isSameGossipVersion(List<GossipManager> managers) {
        Gossip gossip = managers.get(0).localGossip();

        for (GossipManager mgr : managers) {
            if (gossip.compare(mgr.localGossip()) != ComparisonResult.SAME) {
                return false;
            }
        }

        return true;
    }

    private GossipManager createManager(int port) throws Exception {
        return createManager(port, CLUSTER_ID);
    }

    private GossipManager createManager(int port, String cluster) throws Exception {
        InetSocketAddress socketAddress = newSocketAddress(port);

        ClusterAddress address = new ClusterAddress(socketAddress, new ClusterNodeId(0, port));

        ClusterNode node = new DefaultClusterNodeBuilder()
            .withAddress(address)
            .withName("node-" + port)
            .withLocalNode(true)
            .createNode();

        FailureDetector heartbeats = new FailureDetectorMock(node, nodeFailures);

        return new GossipManager(cluster, node, 0, heartbeats, new GossipListener() {
            @Override
            public void onJoinReject(ClusterAddress rejectedBy, String reason) {
                say("Join rejected on " + node + " by " + rejectedBy + " with reason '" + reason + '\'');
            }

            @Override
            public void onStatusChange(GossipNodeStatus oldStatus, GossipNodeStatus newStatus, int order, Set<ClusterNode> topology) {
                say("Status change on " + node + " " + oldStatus + " -> " + newStatus + " (order=" + order + ')');
            }

            @Override
            public void onTopologyChange(Set<ClusterNode> oldTopology, Set<ClusterNode> newTopology) {
                say("Topology  change on " + node + " " + oldTopology + " -> " + newTopology);
            }

            @Override
            public void onKnownAddressesChange(Set<ClusterAddress> oldAddresses, Set<ClusterAddress> newAddresses) {
                say("Known addresses change on " + node + " " + oldAddresses + " -> " + newAddresses);
            }

            @Override
            public void onNodeFailureSuspected(ClusterNode suspected, GossipNodeStatus status) {
                say("Node failure suspected " + suspected + " (status=" + status + ')');
            }

            @Override
            public void onNodeFailureUnsuspected(ClusterNode unsuspected, GossipNodeStatus status) {
                say("Node failure unsuspected " + unsuspected + " (status=" + status + ')');
            }

            @Override
            public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
                say("Node failure " + failed + " (status=" + status + ')');
            }

            @Override
            public void onNodeInconsistency(GossipNodeStatus status) {
                say("Cluster consistency failure on " + node + " (status=" + status + ')');
            }

            @Override
            public Optional<Throwable> onBeforeSend(GossipProtocol msg) {
                return Optional.empty();
            }
        });
    }
}
