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

package io.hekate.coordinate.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterUuid;
import io.hekate.cluster.internal.DefaultClusterHash;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationMember;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.coordinate.internal.CoordinationProtocol.Reject;
import io.hekate.coordinate.internal.CoordinationProtocol.Request;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DefaultCoordinationContextTest extends HekateTestBase {
    private final MessagingChannel<CoordinationProtocol> channel = newChannel();

    private final ExecutorService async = mock(ExecutorService.class);

    private final CoordinationHandler handler = mock(CoordinationHandler.class);

    private final Runnable onComplete = mock(Runnable.class);

    private ClusterTopology topology;

    private DefaultCoordinationContext ctx;

    @Before
    public void setUp() throws Exception {
        topology = newTopology();

        ctx = new DefaultCoordinationContext("test", topology, channel, async, handler, 100, onComplete);
    }

    @Test
    public void testContent() throws Exception {
        assertEquals(topology.getLocalNode(), ctx.getLocalMember().getNode());
        assertTrue(ctx.isCoordinator());
        assertEquals(topology.getNodes().get(0), ctx.getCoordinator().getNode());
        assertEquals(3, ctx.getMembers().size());
        assertEquals(3, ctx.getSize());
        assertTrue(ctx.getMembers().stream().map(CoordinationMember::getNode).allMatch(topology::contains));
        assertEquals(topology, ctx.getTopology());
        assertEquals(topology.getYoungest(), ctx.getMember(topology.getYoungest()).getNode());
        assertEquals(topology.getYoungest().getId(), ctx.getMember(topology.getYoungest()).getNode().getId());
    }

    @Test
    public void testComplete() throws Exception {
        assertFalse(ctx.isDone());

        ctx.coordinate();

        InOrder order = inOrder(handler);

        order.verify(handler).prepare(ctx);
        order.verify(handler).coordinate(ctx);

        ctx.complete();

        assertTrue(ctx.isDone());
        assertFalse(ctx.isCancelled());

        ctx.cancel();
        ctx.halt();

        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testCancel() throws Exception {
        assertFalse(ctx.isDone());
        assertFalse(ctx.isCancelled());

        ctx.cancel();

        assertTrue(ctx.isDone());
        assertTrue(ctx.isCancelled());

        ctx.coordinate();
        ctx.halt();

        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testHalt() throws Exception {
        assertFalse(ctx.isDone());

        ctx.coordinate();

        InOrder order = inOrder(handler);

        order.verify(handler).prepare(ctx);
        order.verify(handler).coordinate(ctx);

        assertFalse(ctx.isDone());

        ctx.halt();

        verifyNoMoreInteractions(handler);

        ctx.cancel();

        assertTrue(ctx.isCancelled());
        assertTrue(ctx.isDone());

        ctx.halt();

        order.verify(handler).cancel(ctx);
    }

    @Test
    public void testProcessMessage() throws Exception {
        ClusterUuid from = topology.getLast().getId();

        Request req = new Request("test", from, topology.getHash(), "message");

        Message<CoordinationProtocol> msg = newRequest(req);

        ctx.coordinate();

        reset(handler);

        ctx.processMessage(msg);

        ArgumentCaptor<CoordinationRequest> captor = ArgumentCaptor.forClass(CoordinationRequest.class);

        verify(handler).process(captor.capture(), eq(ctx));

        assertEquals("message", captor.getValue().get());
        assertEquals(from, captor.getValue().getFrom().getNode().getId());

        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testProcessMessageRejectNotPrepared() throws Exception {
        ClusterUuid from = topology.getLast().getId();

        Request req = new Request("test", from, topology.getHash(), "ignore");

        Message<CoordinationProtocol> msg = newRequest(req);

        ctx.processMessage(msg);

        verify(msg).reply(any(Reject.class));

        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testProcessMessageRejectWrongTopology() throws Exception {
        ClusterUuid from = topology.getLast().getId();

        DefaultClusterHash wrongTopology = new DefaultClusterHash(toSet(newNode()));

        Request req = new Request("test", from, wrongTopology, "ignore");

        Message<CoordinationProtocol> msg = newRequest(req);

        ctx.coordinate();

        reset(handler);

        ctx.processMessage(msg);

        verify(msg).reply(any(Reject.class));

        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testProcessMessageRejectCanceled() throws Exception {
        ClusterUuid from = topology.getNodes().get(topology.getNodes().size() - 1).getId();

        Request req = new Request("test", from, topology.getHash(), "ignore");

        Message<CoordinationProtocol> msg = newRequest(req);

        ctx.coordinate();

        ctx.cancel();

        reset(handler);

        ctx.processMessage(msg);

        verify(msg).reply(any(Reject.class));

        verifyNoMoreInteractions(handler);
    }

    private ClusterTopology newTopology() throws Exception {
        ClusterNode n1 = newLocalNode(newNodeId(1));
        ClusterNode n2 = newNode(newNodeId(2));
        ClusterNode n3 = newNode(newNodeId(3));

        return DefaultClusterTopology.of(1, toSet(n1, n2, n3));
    }

    private Message<CoordinationProtocol> newRequest(Request request) {
        @SuppressWarnings("unchecked")
        Message<CoordinationProtocol> mock = mock(Message.class);

        when(mock.get(Request.class)).thenReturn(request);

        return mock;
    }

    private MessagingChannel<CoordinationProtocol> newChannel() {
        @SuppressWarnings("unchecked")
        MessagingChannel<CoordinationProtocol> mock = mock(MessagingChannel.class);

        when(mock.forNode(Mockito.<ClusterNode>any())).thenReturn(mock);
        when(mock.withFailover(any(FailoverPolicy.class))).thenReturn(mock);
        when(mock.withAffinity(any())).thenReturn(mock);

        return mock;
    }
}
