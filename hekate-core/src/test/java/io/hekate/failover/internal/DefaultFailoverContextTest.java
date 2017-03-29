package io.hekate.failover.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import java.io.IOException;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultFailoverContextTest extends HekateTestBase {
    @Test
    public void test() throws Exception {
        ClusterNode failedNode = newNode();

        DefaultFailoverContext ctx = new DefaultFailoverContext(3, new IOException(), failedNode, singleton(failedNode),
            FailoverRoutingPolicy.RETRY_SAME_NODE);

        assertEquals(3, ctx.getAttempt());
        assertFalse(ctx.isFirstAttempt());
        assertTrue(ctx.isCausedBy(IOException.class));
        assertTrue(ctx.getError() instanceof IOException);
        assertEquals(failedNode, ctx.getFailedNode());
        assertEquals(1, ctx.getFailedNodes().size());
        assertTrue(ctx.getFailedNodes().contains(failedNode));
        assertSame(FailoverRoutingPolicy.RETRY_SAME_NODE, ctx.getRouting());
        assertTrue(ctx.toString(), ctx.toString().startsWith(FailoverContext.class.getSimpleName()));

        assertTrue(ctx.retry().isRetry());
        assertFalse(ctx.fail().isRetry());

        assertSame(FailoverRoutingPolicy.RE_ROUTE, ctx.withRouting(FailoverRoutingPolicy.RE_ROUTE).getRouting());
    }
}
