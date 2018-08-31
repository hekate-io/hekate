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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailoverRequestTest extends FailoverTestBase {
    public FailoverRequestTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testChannelCloseWhileInFailoverWithDelay() throws Exception {
        failures.set(Integer.MAX_VALUE);

        try {
            toRemote.withFailover(ctx -> {
                sender.node().leaveAsync();

                return ctx.retry().withDelay(50);
            }).request("test").response(3, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingException);
            assertEquals("Channel closed [channel=test-channel]", e.getCause().getMessage());
        }
    }

    @Test
    public void testRoutingPolicyReRoute() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.RE_ROUTE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            if (i % 2 == 0) {
                assertEquals(receiver.nodeId(), ctx.failedNode().id());
            } else {
                assertEquals(sender.nodeId(), ctx.failedNode().id());
            }
        }
    }

    @Test
    public void testRoutingPolicyPreferSameNode() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.PREFER_SAME_NODE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> {
            if (details.isFirstAttempt()) {
                receiver.node().leaveAsync().join();

                sender.awaitForTopology(Collections.singletonList(sender));
            }
        });

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            if (i == 0) {
                assertEquals(receiver.nodeId(), ctx.failedNode().id());
            } else {
                assertEquals(sender.nodeId(), ctx.failedNode().id());
            }
        }
    }

    @Test
    public void testRoutingPolicyRetrySameNode() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.RETRY_SAME_NODE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        contexts.forEach(ctx -> assertEquals(receiver.nodeId(), ctx.failedNode().id()));
    }

    @Test
    public void testRemoteFailoverSuccess() throws Exception {
        doFailoverSuccess(toRemote);
    }

    @Test
    public void testSelfFailoverSuccess() throws Exception {
        doFailoverSuccess(toSelf);
    }

    @Test
    public void testRemoteFailoverDelay() throws Exception {
        doFailoverDelay(toRemote);
    }

    @Test
    public void testSelfFailoverDelay() throws Exception {
        doFailoverDelay(toSelf);
    }

    @Test
    public void testRemoteFailoverFailure() throws Exception {
        doFailoverFailure(toRemote, MessagingRemoteException.class);
    }

    @Test
    public void testSelfFailoverFailure() throws Exception {
        doFailoverFailure(toSelf, MessagingRemoteException.class);
    }

    @Test
    public void testNoFailoverOfRoutingErrors() throws Exception {
        AtomicInteger failoverCalls = new AtomicInteger();

        ClusterNodeId unknown = newNodeId();

        try {
            sender.get().forNode(unknown)
                .withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.retry().withReRoute();
                })
                .request("error").response(3, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(EmptyTopologyException.class));
        }

        assertEquals(0, failoverCalls.get());
    }

    private List<FailoverContext> testWithRoutingPolicy(FailoverRoutingPolicy policy, Consumer<FailureInfo> onFailover)
        throws Exception {
        List<FailoverContext> contexts = Collections.synchronizedList(new ArrayList<>());

        failures.set(5);

        AtomicReference<TestChannel> lastTried = new AtomicReference<>();

        String response = sender.withLoadBalancer((message, ctx) -> {
            if (lastTried.get() == null || lastTried.get() == sender) {
                lastTried.set(receiver);
            } else {
                lastTried.set(sender);
            }

            return lastTried.get().nodeId();
        }).withFailover(ctx -> {
            contexts.add(ctx);

            onFailover.accept(ctx);

            return ctx.retry().withRoutingPolicy(policy);
        }).request("test").response(3, TimeUnit.SECONDS);

        assertEquals(5, contexts.size());
        assertEquals("test-" + RETRANSMIT_SUFFIX, response);

        return contexts;
    }

    private void doFailoverSuccess(MessagingChannel<String> channel) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            String response = channel.withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).request("test").response(3, TimeUnit.SECONDS);

            assertNotNull(response);
            assertEquals("test-" + RETRANSMIT_SUFFIX, response);

            assertEquals(attempts, failoverCalls.get());
        });
    }

    private void doFailoverDelay(MessagingChannel<String> channel) throws Exception {
        int failoverDelay = 200;

        failures.set(3);

        List<Long> times = Collections.synchronizedList(new ArrayList<>());

        String response = channel.withFailover(context -> {
            long time = System.nanoTime();

            times.add(time);

            return context.retry().withDelay(failoverDelay);
        }).request("test").response(3, TimeUnit.SECONDS);

        assertNotNull(response);

        assertEquals(3, times.size());
        assertEquals("test-" + RETRANSMIT_SUFFIX, response);

        long prevTime = 0;

        for (Long time : times) {
            if (prevTime != 0) {
                assertTrue(time - prevTime >= TimeUnit.MILLISECONDS.toNanos(failoverDelay));
            }

            prevTime = time;
        }
    }

    private void doFailoverFailure(MessagingChannel<String> channel, Class<? extends Throwable> errorType) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger failoverCalls = new AtomicInteger();

            try {
                channel.withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.attempt() < attempts ? context.retry() : context.fail();
                }).request("test").response(3, TimeUnit.SECONDS);

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(errorType));
            }

            assertEquals(attempts + 1 /* <- we count last failed attempts too. */, failoverCalls.get());
        });
    }
}
