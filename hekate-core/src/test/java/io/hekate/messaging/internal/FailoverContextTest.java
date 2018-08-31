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

import io.hekate.failover.FailoverContext;
import io.hekate.messaging.MessagingRemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class FailoverContextTest extends FailoverTestBase {
    public FailoverContextTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<FailoverContext> contexts = Collections.synchronizedList(new ArrayList<>());

        failures.set(3);

        toRemote.withFailover(ctx -> {
            contexts.add(ctx);

            return ctx.retry();
        }).request("test--1").response(3, TimeUnit.SECONDS);

        assertEquals(3, contexts.size());

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            assertEquals(i, ctx.attempt());
            assertSame(MessagingRemoteException.class, ctx.error().getClass());
            assertEquals(receiver.node().localNode(), ctx.failedNode());
        }
    }
}
