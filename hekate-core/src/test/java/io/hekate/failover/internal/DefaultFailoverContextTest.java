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

        assertEquals(3, ctx.attempt());
        assertFalse(ctx.isFirstAttempt());
        assertTrue(ctx.isCausedBy(IOException.class));
        assertTrue(ctx.error() instanceof IOException);
        assertEquals(failedNode, ctx.failedNode());
        assertEquals(1, ctx.allFailedNodes().size());
        assertTrue(ctx.allFailedNodes().contains(failedNode));
        assertTrue(ctx.isFailed(failedNode));
        assertSame(FailoverRoutingPolicy.RETRY_SAME_NODE, ctx.routing());
        assertTrue(ctx.toString(), ctx.toString().startsWith(FailoverContext.class.getSimpleName()));

        assertTrue(ctx.retry().isRetry());
        assertFalse(ctx.fail().isRetry());

        assertSame(FailoverRoutingPolicy.RE_ROUTE, ctx.withRouting(FailoverRoutingPolicy.RE_ROUTE).routing());
    }
}
