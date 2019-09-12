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
import io.hekate.messaging.operation.BroadcastCallback;
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.BroadcastResult;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class BroadcastContextTest extends HekateTestBase {
    private static final String TEST_MESSAGE = "test";

    private ClusterNode n1;

    private ClusterNode n2;

    private ClusterNode n3;

    @Before
    public void setUp() throws Exception {
        n1 = newNode();
        n2 = newNode();
        n3 = newNode();
    }

    @Test
    public void testToString() {
        BroadcastContext<String> ctx = ctx(allNodes());

        assertEquals(ToString.format(BroadcastResult.class, ctx), ctx.toString());
    }

    @Test
    public void testInitialState() {
        BroadcastContext<String> ctx = ctx(allNodes());

        assertEquals(allNodes(), ctx.nodes());
        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testMessage() {
        BroadcastContext<String> ctx = ctx(allNodes());

        assertEquals(TEST_MESSAGE, ctx.message());
    }

    @Test
    public void testNodes() {
        assertEquals(allNodes(), ctx(allNodes()).nodes());

        assertEquals(singletonList(n1), ctx(singletonList(n1)).nodes());

        BroadcastContext<String> ctx = ctx(allNodes());

        assertFalse(ctx.forgetNode(n1));
        assertEquals(asList(n2, n3), ctx.nodes());

        assertFalse(ctx.forgetNode(n2));
        assertEquals(singletonList(n3), ctx.nodes());

        assertTrue(ctx.forgetNode(n3));

        assertTrue(ctx.nodes().isEmpty());
    }

    @Test
    public void testOnSendSuccess() {
        BroadcastContext<String> ctx = ctx(allNodes());

        assertFalse(ctx.onSendSuccess());
        assertFalse(ctx.onSendSuccess());
        assertTrue(ctx.onSendSuccess());

        assertTrue(ctx.isSuccess());

        allNodes().forEach(n ->
            assertTrue(ctx.isSuccess(n))
        );

        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testOnSendFailure() {
        BroadcastContext<String> ctx = ctx(allNodes());

        Exception err1 = new Exception();
        Exception err2 = new Exception();
        Exception err3 = new Exception();

        assertFalse(ctx.onSendFailure(n1, err1));
        assertFalse(ctx.onSendFailure(n2, err2));

        assertTrue(ctx.onSendFailure(n3, err3));

        assertEquals(new HashSet<>(allNodes()), ctx.errors().keySet());
        assertSame(err1, ctx.errorOf(n1));
        assertSame(err2, ctx.errorOf(n2));
        assertSame(err3, ctx.errorOf(n3));

        assertFalse(ctx.isSuccess());
        assertFalse(ctx.isSuccess(n1));
        assertFalse(ctx.isSuccess(n2));
        assertFalse(ctx.isSuccess(n3));
    }

    @Test
    public void testComplete() {
        BroadcastContext<String> ctx = ctx(allNodes());

        ctx.complete();

        assertTrue(ctx.isSuccess());
        assertFalse(ctx.nodes().isEmpty());
        assertTrue(ctx.errors().isEmpty());

        BroadcastContext<String> errCtx = ctx(allNodes());

        errCtx.future().whenComplete((stringBroadcastResult, throwable) -> {
            throw TEST_ERROR;
        });

        errCtx.complete();
    }

    private BroadcastContext<String> ctx(List<ClusterNode> nodes) {
        return new BroadcastContext<>(TEST_MESSAGE, new ArrayList<>(nodes), new BroadcastFuture<>());
    }

    private List<ClusterNode> allNodes() {
        return asList(n1, n2, n3);
    }

    @SuppressWarnings("unchecked")
    private BroadcastCallback<String> callbackMock() {
        return mock(BroadcastCallback.class);
    }
}
