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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterUuid;
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

        InetSocketAddress address = m.getNodeAddress().getSocket();

        assertFalse(m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, address)).isAccept());
    }

    @Test
    public void testRejectJoinRequestFromAnotherCluster() throws Exception {
        GossipManager m = createManager(1);

        InetSocketAddress address = m.getNodeAddress().getSocket();

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID + "_another", address));

        assertNotNull(reply);
        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.PERMANENT, reply.asReject().getRejectType());
    }

    @Test
    public void testDoNotJoinIfJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.getStatus());

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
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        for (InetSocketAddress addr : seedNodes) {
            m.processJoinReject(JoinReject.retryLater(newAddress(addr), m.getNodeAddress(), addr));
        }

        m.join(seedNodes);

        assertSame(GossipNodeStatus.UP, m.getStatus());
    }

    @Test
    public void testNoSelfJoinAfterTryMultipleSeedNodes() throws Exception {
        GossipManager m = createManager(10000);

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        seedNodes.add(newSocketAddress(2));
        seedNodes.add(newSocketAddress(3));
        seedNodes.add(newSocketAddress(4));

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        for (InetSocketAddress addr : seedNodes) {
            m.processJoinReject(JoinReject.retryLater(newAddress(addr), m.getNodeAddress(), addr));
        }

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.getStatus());
    }

    @Test
    public void testJoinRetryOfFailedSeedNode() throws Exception {
        GossipManager m = createManager(10000);

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        seedNodes.add(newSocketAddress(2));
        seedNodes.add(newSocketAddress(3));
        seedNodes.add(newSocketAddress(4));

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        InetSocketAddress failed = seedNodes.remove(0);

        JoinRequest request = m.processJoinFailure(new JoinRequest(m.getNode(), CLUSTER_ID, failed));

        assertEquals(seedNodes.toString(), failed, request.getToAddress());

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        seedNodes.add(0, failed);

        for (InetSocketAddress addr : seedNodes) {
            assertEquals(addr, m.join(seedNodes).getToAddress());
        }

        assertSame(GossipNodeStatus.DOWN, m.getStatus());
    }

    @Test
    public void testDoNotProcessJoinFailureIfJoined() throws Exception {
        GossipManager m = createManager(1);

        InetSocketAddress failed = newSocketAddress(2);

        assertEquals(failed, m.join(Collections.singletonList(failed)).getToAddress());

        assertEquals(failed, m.processJoinFailure(new JoinRequest(m.getNode(), CLUSTER_ID, failed)).getToAddress());

        assertNull(m.join(Collections.singletonList(failed)));

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertNull(m.processJoinFailure(new JoinRequest(m.getNode(), CLUSTER_ID, failed)));
    }

    @Test
    public void testDoNotProcessJoinAcceptIfJoined() throws Exception {
        GossipManager fake = createManager(2);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertNull(m.processJoinAccept(new JoinAccept(fake.getNodeAddress(), m.getNodeAddress(), fake.getLocalGossip())));

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertEquals(1, m.getLocalGossip().getMembers().size());
        assertTrue(m.getLocalGossip().getMembers().containsKey(m.getId()));
    }

    @Test
    public void testDoNotProcessJoinRejectIfJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertNull(m.processJoinReject(JoinReject.retryLater(newAddress(1), m.getNodeAddress(), newSocketAddress())));

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertEquals(1, m.getLocalGossip().getMembers().size());
        assertTrue(m.getLocalGossip().getMembers().containsKey(m.getId()));
    }

    @Test
    public void testDoNotProcessJoinRequestIfNotJoined() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.singletonList(newSocketAddress(2)));

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertFalse(m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.getNodeAddress().getSocket())).isAccept());

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertTrue(m.getLocalGossip().getMembers().isEmpty());
    }

    @Test
    public void testDoNotProcessJoinRequestFromSelf() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.singletonList(newSocketAddress(2)));

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertFalse(m.processJoinRequest(new JoinRequest(m.getNode(), CLUSTER_ID, m.getNodeAddress().getSocket())).isAccept());

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertTrue(m.getLocalGossip().getMembers().isEmpty());
    }

    @Test
    public void testDoNotProcessJoinRequestIfDownAfterLeave() throws Exception {
        GossipManager m = createManager(1);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.getStatus());

        m.leave();

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.getNodeAddress().getSocket()));

        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.TEMPORARY, reply.asReject().getRejectType());
    }

    @Test
    public void testDoNotProcessJoinRequestFromOtherCluster() throws Exception {
        GossipManager m = createManager(1, CLUSTER_ID + "-other");

        m.join(Collections.emptyList());

        JoinReply reply = m.processJoinRequest(new JoinRequest(newNode(), CLUSTER_ID, m.getNodeAddress().getSocket()));

        assertFalse(reply.isAccept());
        assertSame(JoinReject.RejectType.PERMANENT, reply.asReject().getRejectType());
    }

    @Test
    public void testDoNotProcessJoinAcceptIfNotMember() throws Exception {
        GossipManager fake = createManager(2);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(1);

        m.join(Collections.singletonList(fake.getNodeAddress().getSocket()));

        assertNull(m.processJoinAccept(new JoinAccept(fake.getNodeAddress(), m.getNodeAddress(), fake.getLocalGossip())));

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertTrue(m.getLocalGossip().getMembers().isEmpty());
    }

    @Test
    public void testDoNotProcessUpdateIfDown() throws Exception {
        GossipManager fake = createManager(1);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m.getStatus());

        assertNull(m.processUpdate(new Update(fake.getNodeAddress(), m.getNodeAddress(), fake.getLocalGossip())));

        assertSame(GossipNodeStatus.DOWN, m.getStatus());
        assertTrue(m.getLocalGossip().getMembers().isEmpty());
    }

    @Test
    public void testDoNotProcessUpdateIfNotInRemoteView() throws Exception {
        GossipManager fake = createManager(1);

        fake.join(Collections.emptyList());

        GossipManager m = createManager(2);

        m.join(Collections.emptyList());

        assertSame(GossipNodeStatus.UP, m.getStatus());

        assertNull(m.processUpdate(new Update(fake.getNodeAddress(), m.getNodeAddress(), fake.getLocalGossip())));

        assertSame(GossipNodeStatus.UP, m.getStatus());
        assertEquals(1, m.getLocalGossip().getMembers().size());
        assertTrue(m.getLocalGossip().getMembers().containsKey(m.getId()));
    }

    @Test
    public void testJoinTwoNodes() throws Exception {
        GossipManager m1 = createManager(1);
        GossipManager m2 = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m1.getStatus());
        assertSame(GossipNodeStatus.DOWN, m1.getStatus());

        assertNull(m1.join(Collections.emptyList()));

        assertSame(GossipNodeStatus.UP, m1.getStatus());

        JoinRequest join = m2.join(Collections.singletonList(m1.getNodeAddress().getSocket()));

        assertNotNull(join);
        assertEquals(m2.getNodeAddress(), join.getFrom());
        assertEquals(m1.getNodeAddress().getSocket(), join.getToAddress());

        say("Hey bro! Welcome!");

        JoinAccept rep = m1.processJoinRequest(join).asAccept();

        assertNotNull(rep);
        assertEquals(m1.getNodeAddress(), rep.getFrom());
        assertEquals(m2.getNodeAddress(), rep.getTo());
        assertEquals(2, rep.getGossip().getMembers().size());
        assertTrue(rep.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(rep.getGossip().getMembers().containsKey(m2.getId()));
        assertEquals(1, rep.getGossip().getSeen().size());
        assertTrue(rep.getGossip().getSeen().contains(m1.getId()));

        say("Tnx bro! I'll confirm that I've seen it.");

        Update gos = m2.processJoinAccept(rep);

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertEquals(2, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));
        assertSame(GossipNodeStatus.JOINING, m2.getStatus());

        say("...o-o-kay, looks like we have the same view. I'll put you to UP state.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        say("Ok, I'm UP, cool! Now I'll confirm that I've seen it.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertEquals(2, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));
        assertSame(GossipNodeStatus.UP, m2.getStatus());

        say("We are done.");

        assertNull(m1.processUpdate(gos));

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());
    }

    @Test
    public void testJoinThreeNodes() throws Exception {
        List<GossipManager> nodes = startNodes(3);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);
        GossipManager m3 = nodes.get(2);

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());
        assertSame(GossipNodeStatus.UP, m3.getStatus());

        assertTrue(m1.getLocalGossip().isConvergent());
        assertTrue(m2.getLocalGossip().isConvergent());
        assertTrue(m3.getLocalGossip().isConvergent());

        Gossip g1 = m1.getLocalGossip();

        say("Gossip " + m1.getNode() + ": " + g1);

        assertEquals(3, g1.getMembers().size());
        assertSame(GossipNodeStatus.UP, g1.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g1.getMember(m2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g1.getMember(m3.getId()).getStatus());
        assertEquals(0, g1.getSuspectedView().getSuspected().size());
        assertSame(ComparisonResult.SAME, g1.compare(m2.getLocalGossip()));
        assertSame(ComparisonResult.SAME, g1.compare(m3.getLocalGossip()));
        assertEquals(3, g1.getSeen().size());
        assertTrue(g1.getSeen().contains(m1.getId()));
        assertTrue(g1.getSeen().contains(m2.getId()));
        assertTrue(g1.getSeen().contains(m3.getId()));

        Gossip g2 = m2.getLocalGossip();

        say("Gossip " + m2.getNode() + ": " + g2);

        assertEquals(3, g2.getMembers().size());
        assertSame(GossipNodeStatus.UP, g2.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g2.getMember(m2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g2.getMember(m3.getId()).getStatus());
        assertEquals(0, g2.getSuspectedView().getSuspected().size());
        assertSame(ComparisonResult.SAME, g2.compare(m1.getLocalGossip()));
        assertSame(ComparisonResult.SAME, g2.compare(m3.getLocalGossip()));
        assertEquals(3, g2.getSeen().size());
        assertTrue(g2.getSeen().contains(m1.getId()));
        assertTrue(g2.getSeen().contains(m2.getId()));
        assertTrue(g2.getSeen().contains(m3.getId()));

        Gossip g3 = m3.getLocalGossip();

        say("Gossip " + m3.getNode() + ": " + g3);

        assertEquals(3, g3.getMembers().size());
        assertSame(GossipNodeStatus.UP, g3.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m3.getId()).getStatus());
        assertEquals(0, g3.getSuspectedView().getSuspected().size());
        assertSame(ComparisonResult.SAME, g3.compare(m1.getLocalGossip()));
        assertSame(ComparisonResult.SAME, g3.compare(m2.getLocalGossip()));
        assertEquals(3, g3.getSeen().size());
        assertTrue(g3.getSeen().contains(m1.getId()));
        assertTrue(g3.getSeen().contains(m2.getId()));
        assertTrue(g3.getSeen().contains(m3.getId()));
    }

    @Test
    public void testLeave() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());

        m2.leave();

        say("Sorry Bro, I have to go...");

        Update gos = m2.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));

        say("Well, ok, let me set you to DOWN state.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        say("Ok, I'm in DOWN state.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(2, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));

        say("Before going, I'll confirm that I've seen it.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(1, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.DOWN, m2.getStatus());

        m2.leave();

        assertSame(GossipNodeStatus.DOWN, m2.getStatus());

        UpdateBase dig = m2.gossip();

        assertNotNull(dig);
        assertEquals(m2.getNodeAddress(), dig.getFrom());
        assertEquals(m1.getNodeAddress(), dig.getTo());
        assertEquals(2, dig.getGossipBase().getMembersInfo().size());
        assertTrue(dig.getGossipBase().getMembersInfo().containsKey(m1.getId()));
        assertTrue(dig.getGossipBase().getMembersInfo().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, dig.getGossipBase().getMembersInfo().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, dig.getGossipBase().getMembersInfo().get(m2.getId()).getStatus());
    }

    @Test
    public void testLeaveSameTime() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());

        m1.leave();
        m2.leave();

        say("Sorry Bro, I have to go...");

        Update gos = m2.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));

        assertSame(GossipNodeStatus.LEAVING, m2.getStatus());

        say("Oh! Me too!");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        assertSame(GossipNodeStatus.LEAVING, m1.getStatus());

        say("Well, ok, now I know that you are leaving too.");

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.LEAVING, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(2, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));

        say("Bye then.");

        gos = m1.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        assertSame(GossipNodeStatus.DOWN, m1.getStatus());

        gos = m2.processUpdate(gos).asUpdate();

        assertNotNull(gos);
        assertEquals(m2.getNodeAddress(), gos.getFrom());
        assertEquals(m1.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(2, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));
        assertTrue(gos.getGossip().getSeen().contains(m2.getId()));

        assertSame(GossipNodeStatus.DOWN, m2.getStatus());
    }

    @Test
    public void testFailWithTwoNodes() throws Exception {
        List<GossipManager> nodes = startNodes(2);

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());

        nodeFailures.markFailed(m1.getId(), m2.getId());

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

        assertSame(GossipNodeStatus.UP, m1.getStatus());
        assertSame(GossipNodeStatus.UP, m2.getStatus());
        assertSame(GossipNodeStatus.UP, m3.getStatus());

        say("...I'm dying ...alone ...in the darkness!!!");

        nodeFailures.markFailed(m1.getId(), m2.getId());
        nodeFailures.markFailed(m3.getId(), m2.getId());

        say("Lets see how is he doing...");

        assertTrue(m1.checkAliveness());
        assertTrue(m3.checkAliveness());

        // Should detect failures only once.
        assertFalse(m1.checkAliveness());
        assertFalse(m3.checkAliveness());

        Gossip g1 = m1.getLocalGossip();

        assertEquals(3, g1.getMembers().size());
        assertSame(GossipNodeStatus.UP, g1.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.DOWN, g1.getMember(m2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g1.getMember(m3.getId()).getStatus());
        assertEquals(1, g1.getSuspectedView().getSuspected().size());
        assertTrue(g1.getSuspectedView().getSuspecting(m2.getId()).contains(m1.getId()));

        Gossip g3 = m3.getLocalGossip();

        assertEquals(3, g3.getMembers().size());
        assertSame(GossipNodeStatus.UP, g3.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m3.getId()).getStatus());
        assertEquals(1, g3.getSuspectedView().getSuspected().size());
        assertTrue(g3.getSuspectedView().getSuspecting(m2.getId()).contains(m3.getId()));

        say("OMG! He is dead! ...lets get rid of his body.");

        gossipTillSameVersion(m1, m3);

        g1 = m1.getLocalGossip();

        assertEquals(2, g1.getMembers().size());
        assertSame(GossipNodeStatus.UP, g1.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g1.getMember(m3.getId()).getStatus());
        assertEquals(1, g1.getSuspectedView().getSuspected().size());
        assertFalse(g1.getSuspectedView().getSuspecting(m2.getId()).contains(m1.getId()));
        assertTrue(g1.getSuspectedView().getSuspecting(m2.getId()).contains(m3.getId()));

        g3 = m3.getLocalGossip();

        assertEquals(2, g3.getMembers().size());
        assertSame(GossipNodeStatus.UP, g3.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m3.getId()).getStatus());
        assertEquals(1, g3.getSuspectedView().getSuspected().size());
        assertFalse(g3.getSuspectedView().getSuspecting(m2.getId()).contains(m1.getId()));
        assertTrue(g3.getSuspectedView().getSuspecting(m2.getId()).contains(m3.getId()));

        say("Hm, one more thing. We need to remove evidences of his existence.");

        assertFalse(m1.checkAliveness());

        assertTrue(m3.checkAliveness());

        // Should detect failures only once.
        assertFalse(m3.checkAliveness());

        gossipTillSameVersion(m1, m3);

        g1 = m1.getLocalGossip();

        assertEquals(2, g1.getMembers().size());
        assertSame(GossipNodeStatus.UP, g1.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g1.getMember(m3.getId()).getStatus());
        assertEquals(0, g1.getSuspectedView().getSuspected().size());

        g3 = m3.getLocalGossip();

        assertEquals(2, g3.getMembers().size());
        assertSame(GossipNodeStatus.UP, g3.getMember(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(m3.getId()).getStatus());
        assertEquals(0, g3.getSuspectedView().getSuspected().size());
    }

    @Test
    public void testFailOnJoin() throws Exception {
        GossipManager m1 = createManager(1);
        GossipManager m2 = createManager(2);

        assertSame(GossipNodeStatus.DOWN, m1.getStatus());
        assertSame(GossipNodeStatus.DOWN, m1.getStatus());

        assertNull(m1.join(Collections.emptyList()));

        assertSame(GossipNodeStatus.UP, m1.getStatus());

        say("Hey bro! Let me join!");

        JoinRequest join = m2.join(Collections.singletonList(m1.getNodeAddress().getSocket()));

        assertNotNull(join);
        assertEquals(m2.getNodeAddress(), join.getFrom());
        assertEquals(m1.getNodeAddress().getSocket(), join.getToAddress());

        m1.processJoinRequest(join);

        say("Whoops! I failed.");

        nodeFailures.markFailed(m1.getId(), m2.getId());

        Update gos = m1.gossip().asUpdate();

        assertNotNull(gos);
        assertEquals(m1.getNodeAddress(), gos.getFrom());
        assertEquals(m2.getNodeAddress(), gos.getTo());
        assertEquals(2, gos.getGossip().getMembers().size());
        assertTrue(gos.getGossip().getMembers().containsKey(m1.getId()));
        assertTrue(gos.getGossip().getMembers().containsKey(m2.getId()));
        assertSame(GossipNodeStatus.UP, gos.getGossip().getMembers().get(m1.getId()).getStatus());
        assertSame(GossipNodeStatus.JOINING, gos.getGossip().getMembers().get(m2.getId()).getStatus());
        assertEquals(1, gos.getGossip().getSeen().size());
        assertTrue(gos.getGossip().getSeen().contains(m1.getId()));

        m1.checkAliveness();

        assertNull(m1.gossip());

        Gossip m1Gos = m1.getLocalGossip();

        assertEquals(1, m1Gos.getMembers().size());
        assertTrue(m1Gos.getMembers().containsKey(m1.getId()));
        assertSame(GossipNodeStatus.UP, m1Gos.getMembers().get(m1.getId()).getStatus());
        assertEquals(1, m1Gos.getSeen().size());
        assertTrue(m1Gos.getSeen().contains(m1.getId()));
    }

    @Test
    public void testUpdateDigest() throws Exception {
        List<GossipManager> nodes = startNodes(3);

        Map<ClusterAddress, GossipManager> nodesById = new HashMap<>();

        nodes.forEach(n -> nodesById.put(n.getNodeAddress(), n));

        nodes.forEach(n -> assertSame(GossipNodeStatus.UP, n.getStatus()));

        nodes.forEach(n -> {
            UpdateBase msg = n.gossip();

            // Check that it is a gossip digest.
            assertNull(msg.asUpdate());

            UpdateBase rsp = nodesById.get(msg.getTo()).processUpdate(msg);

            // Should not reply if has the same gossip version.
            assertNull(rsp);
        });

        GossipManager m1 = nodes.get(0);
        GossipManager m2 = nodes.get(1);

        m1.leave();

        while (true) {
            UpdateBase g1 = m2.gossip();

            if (g1.getTo().equals(m1.getNodeAddress())) {
                // Must be digest.
                assertNull(g1.asUpdate());

                UpdateBase g2 = m1.processUpdate(g1);

                // m1 has later version -> must reply with gossip update.
                assertEquals(m2.getNodeAddress(), g2.getTo());
                assertNotNull(g2.asUpdate());

                UpdateBase g3 = m2.processUpdate(g2);

                // Must send a gossip update to update seen list.
                assertSame(m1.getNodeAddress(), g3.getTo());
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
                JoinRequest join = mgr.join(Collections.singletonList(first.getNodeAddress().getSocket()));

                JoinAccept reply = first.processJoinRequest(join).asAccept();

                Update gossip = mgr.processJoinAccept(reply);

                processGossipConversation(first, mgr, gossip);
            }
        }

        gossipTillConvergence(nodes);

        nodes.sort(Comparator.comparing(GossipManager::getNode));

        say("Started: " + nodes.stream().map(GossipManager::getNode).collect(Collectors.toList()));

        return nodes;
    }

    private void gossipTillSameVersion(GossipManager... nodes) {
        assertNotNull(nodes);

        gossipTillSameVersion(Arrays.asList(nodes));
    }

    private void gossipTillSameVersion(List<GossipManager> nodes) {
        assertTrue(nodes.size() > 1);

        Map<ClusterUuid, GossipManager> nodesById = nodes.stream().collect(Collectors.toMap(GossipManager::getId, m -> m));

        int i = 0;

        while (true) {
            say("Round: " + i);

            i++;

            int j = 0;

            for (GossipManager mgr : nodes) {
                say("Iteration: " + i + "-" + j++);

                UpdateBase gossip = mgr.gossip();

                GossipManager to = nodesById.get(gossip.getTo().getId());

                assertNotNull("Unexpected gossip target.", to);

                processGossipConversation(to, mgr, gossip);
            }

            if (isSameGossipVersion(nodes)) {
                break;
            }
        }
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
            if (!m.getLocalGossip().isConvergent()) {
                return false;
            }
        }

        return true;
    }

    private void processGossipConversation(GossipManager m1, GossipManager m2, UpdateBase gossip) {
        while (gossip != null) {
            if (gossip.getTo().equals(m1.getNodeAddress())) {
                gossip = m1.processUpdate(gossip);
            } else {
                gossip = m2.processUpdate(gossip);
            }
        }
    }

    private boolean isSameGossipVersion(List<GossipManager> managers) {
        Gossip gossip = managers.get(0).getLocalGossip();

        for (GossipManager mgr : managers) {
            if (gossip.compare(mgr.getLocalGossip()) != ComparisonResult.SAME) {
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

        ClusterAddress address = new ClusterAddress(socketAddress, new ClusterUuid(0, port));

        ClusterNode node = new DefaultClusterNodeBuilder()
            .withAddress(address)
            .withName("node-" + port)
            .withLocalNode(true)
            .createNode();

        FailureDetector heartbeats = new FailureDetectorMock(node, nodeFailures);

        return new GossipManager(cluster, node, heartbeats, 0, new GossipListener() {
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
        });
    }
}
