package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        AggregateContext<String> ctx = ctx(allNodes(), emptyCallback());

        assertEquals(ToString.format(AggregateResult.class, ctx), ctx.toString());
    }

    @Test
    public void testInitialState() {
        AggregateContext<String> ctx = ctx(allNodes(), emptyCallback());

        synchronized (ctx) {
            assertFalse(ctx.isReady());
        }

        assertEquals(allNodes(), ctx.nodes());
        assertTrue(ctx.errors().isEmpty());
    }

    @Test
    public void testRequest() {
        AggregateContext<String> ctx = ctx(allNodes(), emptyCallback());

        assertEquals(TEST_REQUEST, ctx.request());
    }

    @Test
    public void testNodes() {
        assertEquals(allNodes(), ctx(allNodes(), emptyCallback()).nodes());

        assertEquals(singletonList(n1), ctx(singletonList(n1), emptyCallback()).nodes());

        AggregateContext<String> ctx = ctx(allNodes(), emptyCallback());

        assertFalse(ctx.forgetNode(n1));
        assertEquals(Arrays.asList(n2, n3), ctx.nodes());

        assertFalse(ctx.forgetNode(n2));
        assertEquals(singletonList(n3), ctx.nodes());

        assertTrue(ctx.forgetNode(n3));

        // TODO: Continue from here...
    }

    private AggregateContext<String> ctx(List<ClusterNode> nodes, AggregateCallback<String> callback) {
        return new AggregateContext<>(TEST_REQUEST, nodes, callback);
    }

    private List<ClusterNode> allNodes() {
        return Arrays.asList(n1, n2, n3);
    }

    private AggregateCallback<String> emptyCallback() {
        return (err, result) -> {
            // No-op.
        };
    }
}