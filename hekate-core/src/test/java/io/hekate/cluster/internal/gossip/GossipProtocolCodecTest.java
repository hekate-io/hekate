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
import io.hekate.cluster.internal.DefaultClusterNodeBuilder;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.Update;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateDigest;
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.core.SystemInfo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GossipProtocolCodecTest extends HekateTestBase {
    private final GossipProtocolCodec codec = new GossipProtocolCodec(new AtomicReference<>(newNodeId()));

    @Test
    public void testJoinRequest() throws Exception {
        repeat(3, i -> {
            ClusterNode fromNode = newNode();
            ClusterAddress to = newNode().getAddress();

            JoinRequest before = new JoinRequest(fromNode, "test", to.getNetAddress());

            JoinRequest after = encodeDecode(before);

            assertEquals(fromNode, after.getFromNode());
            assertEquals(fromNode.getAddress(), after.getFrom());
            assertEquals(to.getNetAddress(), after.getToAddress());
            assertEquals(before.getCluster(), after.getCluster());
        });
    }

    @Test
    public void testJoinReject() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            JoinReject before = JoinReject.retryLater(from, to, to.getNetAddress());

            assertSame(JoinReject.RejectType.TEMPORARY, before.getRejectType());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertEquals(to.getNetAddress(), after.getRejectedAddress());
            assertSame(before.getRejectType(), after.getRejectType());
        });
    }

    @Test
    public void testJoinRejectPermanent() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            JoinReject before = JoinReject.permanent(from, to, to.getNetAddress());

            assertSame(JoinReject.RejectType.PERMANENT, before.getRejectType());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertEquals(to.getNetAddress(), after.getRejectedAddress());
            assertSame(before.getRejectType(), after.getRejectType());
        });
    }

    @Test
    public void testJoinRejectFatal() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            JoinReject before = JoinReject.fatal(from, to, to.getNetAddress(), "test reason.");

            assertSame(JoinReject.RejectType.FATAL, before.getRejectType());
            assertNotNull(before.getReason());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertEquals(to.getNetAddress(), after.getRejectedAddress());
            assertSame(before.getRejectType(), after.getRejectType());
            assertEquals(before.getReason(), after.getReason());
        });
    }

    @Test
    public void testJoinAccept() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = newGossip(from, to);

            JoinAccept before = new JoinAccept(from, to, g1);

            JoinAccept after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertEquals(g1.getSeen(), g2.getSeen());
            assertEquals(g1.getAllSuspected(), g2.getAllSuspected());
        });
    }

    @Test
    public void testEmptyUpdate() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = new Gossip();

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertTrue(g2.getSeen().isEmpty());
            assertTrue(g2.getMembers().isEmpty());
            assertTrue(g2.getAllSuspected().isEmpty());
        });
    }

    @Test
    public void testUpdate() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = newGossip(from, to, n -> {
                // No-op (prevent default roles/services values).
            });

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertEquals(g1.getSeen(), g2.getSeen());
            assertEquals(g1.getAllSuspected(), g2.getAllSuspected());

            g1.getMembers().values().forEach(n1 -> {
                ClusterNode n2 = g2.getMember(n1.getId()).getNode();

                assertSystemInfoEquals(n1.getNode().getSysInfo(), n2.getSysInfo());

                assertTrue(n2.getProperties().isEmpty());
                assertTrue(n2.getRoles().isEmpty());
            });
        });
    }

    @Test
    public void testUpdateWithProps() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = newGossip(from, to, n -> {
                Map<String, String> newProps = new HashMap<>();

                newProps.put("p1", UUID.randomUUID().toString());
                newProps.put("p2", UUID.randomUUID().toString());
                newProps.put("p3", "test");

                n.withProperties(newProps);
            });

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertEquals(g1.getSeen(), g2.getSeen());
            assertEquals(g1.getAllSuspected(), g2.getAllSuspected());

            g1.getMembers().values().forEach(n1 -> {
                ClusterNode n2 = g2.getMember(n1.getId()).getNode();

                assertSystemInfoEquals(n1.getNode().getSysInfo(), n2.getSysInfo());

                assertTrue(n2.getRoles().isEmpty());

                assertEquals(n1.getNode().getProperty("p1"), n2.getProperty("p1"));
                assertEquals(n1.getNode().getProperty("p2"), n2.getProperty("p2"));
                assertEquals(n1.getNode().getProperty("p3"), n2.getProperty("p3"));
            });
        });
    }

    @Test
    public void testUpdateWithNullProps() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = newGossip(from, to, n -> {
                Map<String, String> newProps = new HashMap<>();

                newProps.put("p1", UUID.randomUUID().toString());
                newProps.put("p2", UUID.randomUUID().toString());
                newProps.put("p3", "test");
                newProps.put("nullVal", null);
                newProps.put(null, "nullKey");

                n.withProperties(newProps);
            });

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertEquals(g1.getSeen(), g2.getSeen());
            assertEquals(g1.getAllSuspected(), g2.getAllSuspected());

            g1.getMembers().values().forEach(n1 -> {
                ClusterNode n2 = g2.getMember(n1.getId()).getNode();

                assertSystemInfoEquals(n1.getNode().getSysInfo(), n2.getSysInfo());

                assertTrue(n2.getRoles().isEmpty());

                assertEquals(n1.getNode().getProperty("p1"), n2.getProperty("p1"));
                assertEquals(n1.getNode().getProperty("p2"), n2.getProperty("p2"));
                assertEquals(n1.getNode().getProperty("p3"), n2.getProperty("p3"));

                assertNull(n2.getProperty("nullVal"));
                assertTrue(n2.getProperties().containsKey("nullVal"));

                assertEquals("nullKey", n2.getProperty(null));
            });
        });
    }

    @Test
    public void testUpdateWithRoles() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            String uuid = UUID.randomUUID().toString();

            Gossip g1 = newGossip(from, to, n ->
                n.withRoles(new HashSet<>(Arrays.asList("role1", "role2", uuid)))
            );

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossip());

            Gossip g2 = after.getGossip();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertEquals(g1.getSeen(), g2.getSeen());
            assertEquals(g1.getAllSuspected(), g2.getAllSuspected());

            g1.getMembers().values().forEach(n1 -> {
                ClusterNode n2 = g2.getMember(n1.getId()).getNode();

                assertSystemInfoEquals(n1.getNode().getSysInfo(), n2.getSysInfo());

                assertTrue(n2.getProperties().isEmpty());

                assertTrue(n2.getRoles().contains("role1"));
                assertTrue(n2.getRoles().contains("role2"));
                assertTrue(n2.getRoles().contains(uuid));
            });
        });
    }

    @Test
    public void testEmptyUpdateDigest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = new Gossip();

            UpdateDigest before = new UpdateDigest(from, to, new GossipDigest(g1));

            UpdateDigest after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossipBase());

            GossipBase g2 = after.getGossipBase();

            assertSame(ComparisonResult.SAME, g1.compare(g2));
            assertTrue(g2.getMembersInfo().isEmpty());
        });
    }

    @Test
    public void testUpdateDigest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            Gossip g1 = newGossip(from, to);

            UpdateDigest before = new UpdateDigest(from, to, new GossipDigest(g1));

            UpdateDigest after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
            assertNotNull(after.getGossipBase());

            GossipBase g2 = after.getGossipBase();

            assertSame(ComparisonResult.SAME, g1.compare(g2));

            g1.getMembersInfo().values().forEach(n1 -> {
                GossipNodeInfoBase n2 = g2.getMembersInfo().get(n1.getId());

                assertEquals(n1.getId(), n2.getId());
                assertSame(n1.getStatus(), n2.getStatus());
                assertEquals(n1.getVersion(), n2.getVersion());
            });
        });
    }

    @Test
    public void testHeartbeatRequest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            HeartbeatRequest before = new HeartbeatRequest(from, to);

            HeartbeatRequest after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
        });
    }

    @Test
    public void testHeartbeatReply() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().getAddress();
            ClusterAddress to = newNode().getAddress();

            HeartbeatReply before = new HeartbeatReply(from, to);

            HeartbeatReply after = encodeDecode(before);

            assertEquals(from, after.getFrom());
            assertEquals(to, after.getTo());
        });
    }

    private <T extends GossipProtocol> T encodeDecode(T msg) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        StreamDataWriter out = new StreamDataWriter(bout);

        codec.encode(msg, out);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        StreamDataReader in = new StreamDataReader(bin);

        @SuppressWarnings("unchecked")
        T result = (T)codec.decode(in);

        return result;
    }

    private Gossip newGossip(ClusterAddress from, ClusterAddress to) throws Exception {
        return newGossip(from, to, null);
    }

    private Gossip newGossip(ClusterAddress from, ClusterAddress to, Consumer<DefaultClusterNodeBuilder> transform) throws Exception {
        Gossip g1 = new Gossip();

        g1 = g1.update(from.getId(), new GossipNodeState(newNode(transform, from), GossipNodeStatus.JOINING).suspected(from.getId()));
        g1 = g1.update(from.getId(), new GossipNodeState(newNode(transform), GossipNodeStatus.JOINING).suspected(from.getId()));
        g1 = g1.update(from.getId(), new GossipNodeState(newNode(transform), GossipNodeStatus.UP).suspected(to.getId()));
        g1 = g1.update(from.getId(), new GossipNodeState(newNode(transform), GossipNodeStatus.LEAVING));
        g1 = g1.update(from.getId(), new GossipNodeState(newNode(transform), GossipNodeStatus.DOWN));

        return g1;
    }

    private void assertSystemInfoEquals(SystemInfo s1, SystemInfo s2) {
        assertEquals(s1.getCpus(), s2.getCpus());
        assertEquals(s1.getMaxMemory(), s2.getMaxMemory());
        assertEquals(s1.getOsName(), s2.getOsName());
        assertEquals(s1.getOsArch(), s2.getOsArch());
        assertEquals(s1.getOsVersion(), s2.getOsVersion());
        assertEquals(s1.getJvmVersion(), s2.getJvmVersion());
        assertEquals(s1.getJvmName(), s2.getJvmName());
        assertEquals(s1.getJvmVendor(), s2.getJvmVendor());
        assertEquals(s1.getPid(), s2.getPid());
    }
}
