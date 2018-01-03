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
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.unicast.Response;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

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
import static org.mockito.Mockito.when;

public class AggregateContextTest extends HekateTestBase {
    private static final String TEST_REQUEST = "test";

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
        AggregateContext<String> ctx = ctx(allNodes(), callbackMock());

        assertEquals(ToString.format(AggregateResult.class, ctx), ctx.toString());
    }

    @Test
    public void testInitialState() {
        AggregateContext<String> ctx = ctx(allNodes(), callbackMock());

        synchronized (ctx) {
            assertFalse(ctx.isReady());
        }

        assertEquals(allNodes(), ctx.nodes());
        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testRequest() {
        AggregateContext<String> ctx = ctx(allNodes(), callbackMock());

        assertEquals(TEST_REQUEST, ctx.request());
    }

    @Test
    public void testNodes() {
        assertEquals(allNodes(), ctx(allNodes(), callbackMock()).nodes());

        assertEquals(singletonList(n1), ctx(singletonList(n1), callbackMock()).nodes());

        AggregateContext<String> ctx = ctx(allNodes(), callbackMock());

        assertFalse(ctx.forgetNode(n1));
        assertEquals(Arrays.asList(n2, n3), ctx.nodes());

        assertFalse(ctx.forgetNode(n2));
        assertEquals(singletonList(n3), ctx.nodes());

        assertTrue(ctx.forgetNode(n3));

        assertTrue(ctx.nodes().isEmpty());
    }

    @Test
    public void testOnReplySuccess() {
        AggregateContext<String> ctx = ctx(allNodes(), new AggregateCallback<String>() {
            @Override
            public void onComplete(Throwable err, AggregateResult<String> result) {
                // No-op.
            }

            @Override
            public void onReplySuccess(Response<String> rsp, ClusterNode node) {
                throw TEST_ERROR;
            }
        });

        assertFalse(ctx.onReplySuccess(n1, responseMock("r1")));
        assertFalse(ctx.onReplySuccess(n2, responseMock("r2")));

        assertTrue(ctx.onReplySuccess(n3, responseMock("r3")));

        assertTrue(ctx.isSuccess());
        assertTrue(ctx.isSuccess(n1));
        assertTrue(ctx.isSuccess(n2));
        assertTrue(ctx.isSuccess(n3));

        assertTrue(ctx.errors().isEmpty());

        assertEquals("r1", ctx.resultOf(n1));
        assertEquals("r2", ctx.resultOf(n2));
        assertEquals("r3", ctx.resultOf(n3));

        Set<String> results = new HashSet<>(Arrays.asList("r1", "r2", "r3"));

        for (String r : ctx) {
            assertTrue(results.contains(r));
        }

        ctx.stream().forEach(r ->
            assertTrue(results.contains(r))
        );

        assertEquals(results, new HashSet<>(ctx.results()));
    }

    @Test
    public void testOnReplyFailure() {
        AggregateContext<String> ctx = ctx(allNodes(), new AggregateCallback<String>() {
            @Override
            public void onComplete(Throwable err, AggregateResult<String> result) {
                // No-op.
            }

            @Override
            public void onReplyFailure(String request, ClusterNode node, Throwable cause) {
                throw TEST_ERROR;
            }
        });

        Exception err1 = new Exception();
        Exception err2 = new Exception();
        Exception err3 = new Exception();

        assertFalse(ctx.onReplyFailure(n1, err1));
        assertFalse(ctx.onReplyFailure(n2, err2));

        assertTrue(ctx.onReplyFailure(n3, err3));

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
        AggregateCallback<String> callback = callbackMock();

        AggregateContext<String> ctx = ctx(allNodes(), callback);

        ctx.complete();

        verify(callback).onComplete(isNull(), any());
        verifyNoMoreInteractions(callback);

        AggregateContext<String> errCtx = ctx(allNodes(), (err, result) -> {
            throw TEST_ERROR;
        });

        errCtx.complete();
    }

    private AggregateContext<String> ctx(List<ClusterNode> nodes, AggregateCallback<String> callback) {
        return new AggregateContext<>(TEST_REQUEST, nodes, callback);
    }

    private List<ClusterNode> allNodes() {
        return Arrays.asList(n1, n2, n3);
    }

    @SuppressWarnings("unchecked")
    private AggregateCallback<String> callbackMock() {
        return mock(AggregateCallback.class);
    }

    @SuppressWarnings("unchecked")
    private Response<String> responseMock(String response) {
        Response mock = mock(Response.class);

        when(mock.get()).thenReturn(response);

        return mock;
    }
}
