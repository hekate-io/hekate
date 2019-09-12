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
import io.hekate.messaging.retry.FailedAttempt;
import java.io.IOException;
import org.junit.Test;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RETRY_SAME_NODE;
import static io.hekate.messaging.retry.RetryRoutingPolicy.RE_ROUTE;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MessageOperationFailureTest extends HekateTestBase {
    @Test
    public void test() throws Exception {
        ClusterNode failedNode = newNode();

        MessageOperationFailure ctx = new MessageOperationFailure(
            3,
            new IOException(),
            failedNode,
            singleton(failedNode),
            RETRY_SAME_NODE
        );

        assertEquals(3, ctx.attempt());
        assertFalse(ctx.isFirstAttempt());
        assertTrue(ctx.isCausedBy(IOException.class));
        assertTrue(ctx.error() instanceof IOException);
        assertEquals(failedNode, ctx.lastTriedNode());
        assertEquals(1, ctx.allTriedNodes().size());
        assertTrue(ctx.allTriedNodes().contains(failedNode));
        assertTrue(ctx.hasTriedNode(failedNode));
        assertSame(RETRY_SAME_NODE, ctx.routing());
        assertTrue(ctx.toString(), ctx.toString().startsWith(FailedAttempt.class.getSimpleName()));

        assertSame(RE_ROUTE, ctx.withRouting(RE_ROUTE).routing());
    }
}
