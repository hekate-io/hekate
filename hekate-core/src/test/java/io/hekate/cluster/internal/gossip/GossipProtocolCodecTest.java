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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeRuntime;
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
import io.hekate.core.ServiceInfo;
import io.hekate.core.ServiceProperty;
import io.hekate.core.service.Service;
import io.hekate.core.service.internal.DefaultServiceInfo;
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
    private static class TestClass1 {
        // No-op.
    }

    private static class TestClass2 {
        // No-op.
    }

    private static class TestClass3 {
        // No-op.
    }

    private final GossipProtocolCodec codec = new GossipProtocolCodec(new AtomicReference<>(newNodeId()));

    @Test
    public void testJoinRequest() throws Exception {
        repeat(3, i -> {
            ClusterNode fromNode = newNode();
            ClusterAddress to = newNode().address();

            JoinRequest before = new JoinRequest(fromNode, "test", to.socket());

            JoinRequest after = encodeDecode(before);

            assertEquals(fromNode, after.fromNode());
            assertEquals(fromNode.address(), after.from());
            assertEquals(to.socket(), after.toAddress());
            assertEquals(before.cluster(), after.cluster());
        });
    }

    @Test
    public void testJoinReject() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            JoinReject before = JoinReject.retryLater(from, to, to.socket());

            assertSame(JoinReject.RejectType.TEMPORARY, before.rejectType());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertEquals(to.socket(), after.rejectedAddress());
            assertSame(before.rejectType(), after.rejectType());
        });
    }

    @Test
    public void testJoinRejectPermanent() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            JoinReject before = JoinReject.permanent(from, to, to.socket());

            assertSame(JoinReject.RejectType.PERMANENT, before.rejectType());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertEquals(to.socket(), after.rejectedAddress());
            assertSame(before.rejectType(), after.rejectType());
        });
    }

    @Test
    public void testJoinRejectFatal() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            JoinReject before = JoinReject.fatal(from, to, to.socket(), "test reason.");

            assertSame(JoinReject.RejectType.FATAL, before.rejectType());
            assertNotNull(before.reason());

            JoinReject after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertEquals(to.socket(), after.rejectedAddress());
            assertSame(before.rejectType(), after.rejectType());
            assertEquals(before.reason(), after.reason());
        });
    }

    @Test
    public void testJoinAccept() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = newGossip(from, to);

            JoinAccept before = new JoinAccept(from, to, g1);

            JoinAccept after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertEquals(g1.seen(), g2.seen());
            assertEquals(g1.allSuspected(), g2.allSuspected());
        });
    }

    @Test
    public void testEmptyUpdate() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = new Gossip();

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertTrue(g2.seen().isEmpty());
            assertTrue(g2.members().isEmpty());
            assertTrue(g2.allSuspected().isEmpty());
        });
    }

    @Test
    public void testUpdate() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = newGossip(from, to, n -> {
                // No-op (prevent default roles/services values).
            });

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertEquals(g1.seen(), g2.seen());
            assertEquals(g1.allSuspected(), g2.allSuspected());

            g1.members().values().forEach(n1 -> {
                ClusterNode n2 = g2.member(n1.id()).node();

                assertSystemInfoEquals(n1.node().runtime(), n2.runtime());

                assertTrue(n2.properties().isEmpty());
                assertTrue(n2.roles().isEmpty());
            });
        });
    }

    @Test
    public void testUpdateWithProps() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = newGossip(from, to, n -> {
                Map<String, String> newProps = new HashMap<>();

                newProps.put("p1", UUID.randomUUID().toString());
                newProps.put("p2", UUID.randomUUID().toString());
                newProps.put("p3", "test");

                n.withProperties(newProps);
            });

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertEquals(g1.seen(), g2.seen());
            assertEquals(g1.allSuspected(), g2.allSuspected());

            g1.members().values().forEach(n1 -> {
                ClusterNode n2 = g2.member(n1.id()).node();

                assertSystemInfoEquals(n1.node().runtime(), n2.runtime());

                assertTrue(n2.roles().isEmpty());

                assertEquals(n1.node().property("p1"), n2.property("p1"));
                assertEquals(n1.node().property("p2"), n2.property("p2"));
                assertEquals(n1.node().property("p3"), n2.property("p3"));
            });
        });
    }

    @Test
    public void testUpdateServiceWithProps() throws Exception {
        repeat(3, i -> {
            Consumer<DefaultClusterNodeBuilder> transform = node -> {
                Map<String, ServiceInfo> services = new HashMap<>();

                int idx = 0;

                for (Class<?> type : Arrays.asList(TestClass1.class, TestClass2.class, TestClass3.class)) {
                    Map<String, ServiceProperty<?>> serviceProps = new HashMap<>();

                    serviceProps.put("string", ServiceProperty.forString("string", String.valueOf(idx)));
                    serviceProps.put("int", ServiceProperty.forInteger("int", idx));
                    serviceProps.put("long", ServiceProperty.forLong("long", idx + 10000));
                    serviceProps.put("bool", ServiceProperty.forBoolean("bool", idx % 2 == 0));

                    services.put(type.getName(), new DefaultServiceInfo(type.getName(), serviceProps));

                    idx++;
                }

                node.withServices(services);
            };

            ClusterAddress from = newNode(transform).address();
            ClusterAddress to = newNode(transform).address();

            Gossip g1 = newGossip(from, to);

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            for (GossipNodeState n1 : g1.members().values()) {
                ClusterNode n2 = g2.member(n1.id()).node();

                assertServicePropertyEquals(n1.node(), n2);
            }
        });
    }

    @Test
    public void testUpdateWithNullProps() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

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

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertEquals(g1.seen(), g2.seen());
            assertEquals(g1.allSuspected(), g2.allSuspected());

            g1.members().values().forEach(n1 -> {
                ClusterNode n2 = g2.member(n1.id()).node();

                assertSystemInfoEquals(n1.node().runtime(), n2.runtime());

                assertTrue(n2.roles().isEmpty());

                assertEquals(n1.node().property("p1"), n2.property("p1"));
                assertEquals(n1.node().property("p2"), n2.property("p2"));
                assertEquals(n1.node().property("p3"), n2.property("p3"));

                assertNull(n2.property("nullVal"));
                assertTrue(n2.properties().containsKey("nullVal"));

                assertEquals("nullKey", n2.property(null));
            });
        });
    }

    @Test
    public void testUpdateWithRoles() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            String uuid = UUID.randomUUID().toString();

            Gossip g1 = newGossip(from, to, n ->
                n.withRoles(new HashSet<>(Arrays.asList("role1", "role2", uuid)))
            );

            Update before = new Update(from, to, g1);

            Update after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossip());

            Gossip g2 = after.gossip();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertEquals(g1.seen(), g2.seen());
            assertEquals(g1.allSuspected(), g2.allSuspected());

            g1.members().values().forEach(n1 -> {
                ClusterNode n2 = g2.member(n1.id()).node();

                assertSystemInfoEquals(n1.node().runtime(), n2.runtime());

                assertTrue(n2.properties().isEmpty());

                assertTrue(n2.roles().contains("role1"));
                assertTrue(n2.roles().contains("role2"));
                assertTrue(n2.roles().contains(uuid));
            });
        });
    }

    @Test
    public void testEmptyUpdateDigest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = new Gossip();

            UpdateDigest before = new UpdateDigest(from, to, new GossipDigest(g1));

            UpdateDigest after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossipBase());

            GossipBase g2 = after.gossipBase();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));
            assertTrue(g2.membersInfo().isEmpty());
        });
    }

    @Test
    public void testUpdateDigest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            Gossip g1 = newGossip(from, to);

            UpdateDigest before = new UpdateDigest(from, to, new GossipDigest(g1));

            UpdateDigest after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
            assertNotNull(after.gossipBase());

            GossipBase g2 = after.gossipBase();

            assertSame(GossipPrecedence.SAME, g1.compare(g2));

            g1.membersInfo().values().forEach(n1 -> {
                GossipNodeInfoBase n2 = g2.membersInfo().get(n1.id());

                assertEquals(n1.id(), n2.id());
                assertSame(n1.status(), n2.status());
                assertEquals(n1.version(), n2.version());
            });
        });
    }

    @Test
    public void testHeartbeatRequest() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            HeartbeatRequest before = new HeartbeatRequest(from, to);

            HeartbeatRequest after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
        });
    }

    @Test
    public void testHeartbeatReply() throws Exception {
        repeat(3, i -> {
            ClusterAddress from = newNode().address();
            ClusterAddress to = newNode().address();

            HeartbeatReply before = new HeartbeatReply(from, to);

            HeartbeatReply after = encodeDecode(before);

            assertEquals(from, after.from());
            assertEquals(to, after.to());
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

        g1 = g1.update(from.id(), new GossipNodeState(newNode(transform, from), GossipNodeStatus.JOINING).suspect(from.id()));
        g1 = g1.update(from.id(), new GossipNodeState(newNode(transform), GossipNodeStatus.JOINING).suspect(from.id()));
        g1 = g1.update(from.id(), new GossipNodeState(newNode(transform), GossipNodeStatus.UP).suspect(to.id()));
        g1 = g1.update(from.id(), new GossipNodeState(newNode(transform), GossipNodeStatus.LEAVING));
        g1 = g1.update(from.id(), new GossipNodeState(newNode(transform), GossipNodeStatus.DOWN));

        return g1;
    }

    private void assertSystemInfoEquals(ClusterNodeRuntime s1, ClusterNodeRuntime s2) {
        assertEquals(s1.cpus(), s2.cpus());
        assertEquals(s1.maxMemory(), s2.maxMemory());
        assertEquals(s1.osName(), s2.osName());
        assertEquals(s1.osArch(), s2.osArch());
        assertEquals(s1.osVersion(), s2.osVersion());
        assertEquals(s1.jvmVersion(), s2.jvmVersion());
        assertEquals(s1.jvmName(), s2.jvmName());
        assertEquals(s1.jvmVendor(), s2.jvmVendor());
        assertEquals(s1.pid(), s2.pid());
        assertEquals(s1.toString(), s2.toString());
    }

    @SuppressWarnings("unchecked")
    private void assertServicePropertyEquals(ClusterNode n1, ClusterNode n2) throws Exception {
        for (ServiceInfo s1 : n1.services().values()) {
            Class<? extends Service> type = (Class<? extends Service>)Class.forName(s1.type());

            ServiceInfo s2 = n2.service(type);

            Map<String, ServiceProperty<?>> props1 = s1.properties();
            Map<String, ServiceProperty<?>> props2 = s2.properties();

            assertEquals(props1.keySet(), props2.keySet());

            props1.keySet().forEach(name -> {
                ServiceProperty<?> p1 = props1.get(name);
                ServiceProperty<?> p2 = props2.get(name);

                assertEquals(name, p1.name());
                assertEquals(name, p2.name());

                assertEquals(p1.type(), p2.type());
                assertEquals(p1.name(), p2.name());
                assertEquals(p1.value(), p2.value());
            });
        }
    }
}
