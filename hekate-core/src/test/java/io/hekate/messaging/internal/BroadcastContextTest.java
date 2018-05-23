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

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastResult;
import io.hekate.util.format.ToString;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
        BroadcastContext<String> ctx = ctx(allNodes(), callbackMock());

        assertEquals(ToString.format(BroadcastResult.class, ctx), ctx.toString());
    }

    @Test
    public void testInitialState() {
        BroadcastContext<String> ctx = ctx(allNodes(), callbackMock());

        assertEquals(allNodes(), ctx.nodes());
        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testMessage() {
        BroadcastContext<String> ctx = ctx(allNodes(), callbackMock());

        assertEquals(TEST_MESSAGE, ctx.message());
    }

    @Test
    public void testNodes() {
        assertEquals(allNodes(), ctx(allNodes(), callbackMock()).nodes());

        assertEquals(singletonList(n1), ctx(singletonList(n1), callbackMock()).nodes());

        BroadcastContext<String> ctx = ctx(allNodes(), callbackMock());

        assertFalse(ctx.forgetNode(n1));
        assertEquals(asList(n2, n3), ctx.nodes());

        assertFalse(ctx.forgetNode(n2));
        assertEquals(singletonList(n3), ctx.nodes());

        assertTrue(ctx.forgetNode(n3));

        assertTrue(ctx.nodes().isEmpty());
    }

    @Test
    public void testOnSendSuccess() {
        BroadcastContext<String> ctx = ctx(allNodes(), new BroadcastCallback<String>() {
            @Override
            public void onComplete(Throwable err, BroadcastResult<String> result) {
                // No-op.
            }

            @Override
            public void onSendSuccess(String message, ClusterNode node) {
                throw TEST_ERROR;
            }
        });

        assertFalse(ctx.onSendSuccess(n1));
        assertFalse(ctx.onSendSuccess(n2));
        assertTrue(ctx.onSendSuccess(n3));

        assertTrue(ctx.isSuccess());
        assertTrue(ctx.isSuccess(n1));
        assertTrue(ctx.isSuccess(n2));
        assertTrue(ctx.isSuccess(n3));

        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testOnSendFailure() {
        BroadcastContext<String> ctx = ctx(allNodes(), new BroadcastCallback<String>() {
            @Override
            public void onComplete(Throwable err, BroadcastResult<String> result) {
                // No-op.
            }

            @Override
            public void onSendFailure(String message, ClusterNode node, Throwable error) {
                throw TEST_ERROR;
            }
        });

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
        BroadcastCallback<String> callback = callbackMock();

        BroadcastContext<String> ctx = ctx(allNodes(), callback);

        ctx.complete();

        verify(callback).onComplete(isNull(), any());
        verifyNoMoreInteractions(callback);

        BroadcastContext<String> errCtx = ctx(allNodes(), (err, result) -> {
            throw TEST_ERROR;
        });

        errCtx.complete();
    }

    private BroadcastContext<String> ctx(List<ClusterNode> nodes, BroadcastCallback<String> callback) {
        return new BroadcastContext<>(TEST_MESSAGE, nodes, callback);
    }

    private List<ClusterNode> allNodes() {
        return asList(n1, n2, n3);
    }

    @SuppressWarnings("unchecked")
    private BroadcastCallback<String> callbackMock() {
        return mock(BroadcastCallback.class);
    }
}
