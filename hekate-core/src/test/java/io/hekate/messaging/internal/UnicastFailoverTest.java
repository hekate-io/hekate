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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.UnknownRouteException;
import java.nio.channels.ClosedChannelException;
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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnicastFailoverTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private TestChannel sender;

    private TestChannel receiver;

    private MessagingChannel<String> toSelf;

    private MessagingChannel<String> toRemote;

    public UnicastFailoverTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        List<TestChannel> channels = createAndJoinChannels(2, c -> c.withReceiver(msg -> {
            if (failures.getAndDecrement() > 0) {
                throw TEST_ERROR;
            }

            if (msg.mustReply()) {
                msg.reply(msg.get() + "reply");
            }
        }));

        awaitForChannelsTopology(channels);

        sender = channels.get(0);

        receiver = channels.get(1);

        toSelf = sender.get().forNode(sender.getNodeId());
        toRemote = sender.get().forNode(receiver.getNodeId());
    }

    @Test
    public void testContext() throws Exception {
        List<FailoverContext> contexts = Collections.synchronizedList(new ArrayList<>());

        failures.set(3);

        toRemote.withFailover(ctx -> {
            contexts.add(ctx);

            return ctx.retry();
        }).request("test--1").getReply(3, TimeUnit.SECONDS);

        assertEquals(3, contexts.size());

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            assertEquals(i, ctx.getAttempt());
            assertSame(ClosedChannelException.class, ctx.getError().getClass());
            assertEquals(receiver.getInstance().getNode(), ctx.getFailedNode());
        }
    }

    @Test
    public void testChannelCloseWhileInFailoverWithDelay() throws Exception {
        failures.set(Integer.MAX_VALUE);

        try {
            toRemote.withFailover(ctx -> {
                sender.getInstance().leaveAsync();

                return ctx.retry().withDelay(50);
            }).request("test--1").getReply(3, TimeUnit.SECONDS);

            fail("Error eas expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingException);
            assertEquals("Channel closed [channel=test_channel]", e.getCause().getMessage());
        }
    }

    @Test
    public void testRoutingPolicyReRoute() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.RE_ROUTE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            if (i % 2 == 0) {
                assertEquals(receiver.getNodeId(), ctx.getFailedNode().getId());
            } else {
                assertEquals(sender.getNodeId(), ctx.getFailedNode().getId());
            }
        }
    }

    @Test
    public void testRoutingPolicyPreferSameNode() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.PREFER_SAME_NODE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> {
            if (details.isFirstAttempt()) {
                receiver.getInstance().leaveAsync().join();

                sender.awaitForTopology(Collections.singletonList(sender));
            }
        });

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            if (i == 0) {
                assertEquals(receiver.getNodeId(), ctx.getFailedNode().getId());
            } else {
                assertEquals(sender.getNodeId(), ctx.getFailedNode().getId());
            }
        }
    }

    @Test
    public void testRoutingPolicyRetrySameNode() throws Exception {
        FailoverRoutingPolicy policy = FailoverRoutingPolicy.RETRY_SAME_NODE;

        List<FailoverContext> contexts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        contexts.forEach(ctx -> assertEquals(receiver.getNodeId(), ctx.getFailedNode().getId()));
    }

    @Test
    public void testRequestToRemoteFailoverSuccess() throws Exception {
        doTestRequestFailoverSuccess(toRemote);
    }

    @Test
    public void testRequestToSelfFailoverSuccess() throws Exception {
        doTestRequestFailoverSuccess(toSelf);
    }

    @Test
    public void testRequestToRemoteFailoverDelay() throws Exception {
        doTestRequestFailoverDelay(toRemote);
    }

    @Test
    public void testRequestToSelfFailoverDelay() throws Exception {
        doTestRequestFailoverDelay(toSelf);
    }

    @Test
    public void testRequestToRemoteFailoverFailure() throws Exception {
        doTestRequestFailoverFailure(toRemote, ClosedChannelException.class);
    }

    @Test
    public void testRequestToSelfFailoverFailure() throws Exception {
        doTestRequestFailoverFailure(toSelf, AssertionError.class);
    }

    @Test
    public void testRequestNoRoutingFailover() throws Exception {
        AtomicInteger failoverCalls = new AtomicInteger();

        ClusterNodeId unknown = newNodeId();

        try {
            sender.get().forNode(unknown)
                .withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.retry().withReRoute();
                })
                .request("error").getReply(3, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(UnknownRouteException.class));
        }

        assertEquals(0, failoverCalls.get());
    }

    @Test
    public void testSendNoRoutingFailover() throws Exception {
        AtomicInteger failoverCalls = new AtomicInteger();

        ClusterNodeId unknown = newNodeId();

        try {
            sender.get().forNode(unknown)
                .withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.retry().withReRoute();
                })
                .send("error").get(3, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(UnknownRouteException.class));
        }

        assertEquals(0, failoverCalls.get());
    }

    private List<FailoverContext> testWithRoutingPolicy(FailoverRoutingPolicy policy, Consumer<FailureInfo> onFailover)
        throws Exception {
        List<FailoverContext> contexts = Collections.synchronizedList(new ArrayList<>());

        failures.set(5);

        AtomicReference<TestChannel> lastTried = new AtomicReference<>();

        sender.withLoadBalancer((message, ctx) -> {
            if (lastTried.get() == null || lastTried.get() == sender) {
                lastTried.set(receiver);
            } else {
                lastTried.set(sender);
            }

            return lastTried.get().getNodeId();
        }).withFailover(ctx -> {
            contexts.add(ctx);

            onFailover.accept(ctx);

            return ctx.retry().withRoutingPolicy(policy);
        }).request("test").getReply(3, TimeUnit.SECONDS);

        assertEquals(5, contexts.size());

        return contexts;
    }

    private void doTestRequestFailoverSuccess(MessagingChannel<String> channel) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            String reply = channel.withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).request("test").getReply(3, TimeUnit.SECONDS);

            assertNotNull(reply);

            assertEquals(attempts, failoverCalls.get());
        });
    }

    private void doTestRequestFailoverDelay(MessagingChannel<String> channel) throws Exception {
        int failoverDelay = 200;

        failures.set(3);

        List<Long> times = Collections.synchronizedList(new ArrayList<>());

        String reply = channel.withFailover(context -> {
            long time = System.nanoTime();

            times.add(time);

            return context.retry().withDelay(failoverDelay);
        }).request("test").getReply(3, TimeUnit.SECONDS);

        assertNotNull(reply);

        assertEquals(3, times.size());

        long prevTime = 0;

        for (Long time : times) {
            if (prevTime == 0) {
                prevTime = time;
            } else {
                assertTrue(time - prevTime >= TimeUnit.MILLISECONDS.toNanos(failoverDelay));

                prevTime = time;
            }
        }
    }

    private void doTestRequestFailoverFailure(MessagingChannel<String> channel, Class<? extends Throwable> errorType) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger failoverCalls = new AtomicInteger();

            try {
                channel.withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.getAttempt() < attempts ? context.retry() : context.fail();
                }).request("test").getReply(3, TimeUnit.SECONDS);

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(errorType));
            }

            assertEquals(attempts + 1 /* <- we count last failed attempts too. */, failoverCalls.get());
        });
    }
}
