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
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.messaging.broadcast.AggregateResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FailoverAggregateTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private List<TestChannel> channels;

    private TestChannel sender;

    public FailoverAggregateTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        channels = createAndJoinChannels(3, c -> c.withReceiver(msg -> {
            if (failures.getAndDecrement() > 0) {
                throw TEST_ERROR;
            }

            msg.reply(msg.get() + "reply");
        }));

        sender = channels.get(0);
    }

    @Test
    public void testSuccess() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(channels.size() * attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = get(sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).aggregate("test"));

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            assertEquals(channels.size() * attempts, failoverCalls.get());
        });
    }

    @Test
    public void testDelay() throws Exception {
        int failoverDelay = 200;

        failures.set(channels.size() * 3);

        Map<ClusterNodeId, List<Long>> times = new HashMap<>();

        channels.forEach(c -> times.put(c.nodeId(), Collections.synchronizedList(new ArrayList<>())));

        AggregateResult<String> result = get(sender.get().withFailover(ctx -> {
            times.get(ctx.failedNode().id()).add(currentTimeMillis());

            return ctx.retry().withDelay(failoverDelay);
        }).aggregate("test"));

        times.forEach((id, series) -> {
            assertEquals(3, series.size());

            long prevTime = 0;

            for (Long time : series) {
                if (prevTime != 0) {
                    assertTrue(time - prevTime >= failoverDelay);
                }

                prevTime = time;
            }
        });

        assertTrue(result.isSuccess());
        assertEquals(channels.size(), result.results().size());
    }

    @Test
    public void testPartialSuccess() throws Exception {
        repeat(2, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = get(sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).aggregate("test"));

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            assertEquals(attempts, failoverCalls.get());
        });
    }

    @Test
    public void testFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = get(sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.attempt() < attempts ? context.retry() : context.fail();
            }).aggregate("test"));

            assertFalse(result.isSuccess());
            assertTrue(result.results().isEmpty());
            assertEquals(channels.size(), result.errors().size());

            assertEquals(channels.size() * attempts + channels.size() /* <- we count last failed attempts too. */, failoverCalls.get());
        });
    }

    @Test
    public void testPartialFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = get(sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.fail();
            }).aggregate("test"));

            assertFalse(result.isSuccess());
            assertEquals(channels.size() - attempts, result.results().size());
            assertEquals(attempts, result.errors().size());

            assertEquals(attempts, failoverCalls.get());
        });
    }

    @Test
    public void testErrorInFailoverPolicy() throws Exception {
        repeat(3, i -> {
            failures.set(Integer.MAX_VALUE);

            AggregateResult<String> result = get(sender.get().withFailover(context -> {
                throw TEST_ERROR;
            }).aggregate("test"));

            assertFalse(result.isSuccess());
            assertTrue(result.results().isEmpty());
            assertEquals(channels.size(), result.errors().size());
        });
    }

    @Test
    public void testNodeLeaveDuringFailover() throws Exception {
        for (FailoverRoutingPolicy policy : FailoverRoutingPolicy.values()) {
            say("Using policy " + policy);

            repeat(3, i -> {
                failures.set(1);

                AtomicReference<Exception> errRef = new AtomicReference<>();
                AtomicReference<TestChannel> leaveRef = new AtomicReference<>();

                AggregateResult<String> result = get(sender.get().forRemotes().withFailover(context -> {
                    try {
                        TestChannel leave = channels.stream()
                            .filter(c -> c.nodeId().equals(context.failedNode().id()))
                            .findFirst()
                            .orElseThrow(AssertionError::new)
                            .leave();

                        leaveRef.set(leave);
                    } catch (ExecutionException | InterruptedException | TimeoutException e) {
                        errRef.compareAndSet(null, e);
                    }

                    return context.retry().withRoutingPolicy(policy);
                }).aggregate("test"));

                if (errRef.get() != null) {
                    throw new AssertionError(errRef.get());
                }

                assertNotNull(leaveRef.get());
                assertFalse(result.toString(), result.isSuccess());
                assertEquals(result.toString(), 1, result.errors().size());
                assertEquals(result.toString(), channels.size() - 2, result.results().size());

                Throwable expected = result.errors().get(leaveRef.get().node().localNode());

                assertNotNull(result.toString(), expected);

                leaveRef.get().join();

                awaitForChannelsTopology(channels);
            });
        }
    }
}
