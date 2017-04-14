package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.HekateFutureException;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.messaging.broadcast.AggregateResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FailoverAggregateTest extends MessagingServiceTestBase {
    private AtomicInteger failures = new AtomicInteger();

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
            assertEquals(channels.size(), result.getResults().size());

            assertEquals(channels.size() * attempts, failoverCalls.get());
        });
    }

    @Test
    public void testDelay() throws Exception {
        int failoverDelay = 200;

        failures.set(channels.size() * 3);

        Map<ClusterNodeId, List<Long>> times = new HashMap<>();

        channels.forEach(c -> times.put(c.getNodeId(), Collections.synchronizedList(new ArrayList<>())));

        AggregateResult<String> result = get(sender.get().withFailover(ctx -> {
            times.get(ctx.getFailedNode().getId()).add(currentTimeMillis());

            return ctx.retry().withDelay(failoverDelay);
        }).aggregate("test"));

        times.forEach((id, series) -> {
            assertEquals(3, series.size());

            long prevTime = 0;

            for (Long time : series) {
                if (prevTime == 0) {
                    prevTime = time;
                } else {
                    assertTrue(time - prevTime >= failoverDelay);

                    prevTime = time;
                }
            }
        });

        assertTrue(result.isSuccess());
        assertEquals(channels.size(), result.getResults().size());
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
            assertEquals(channels.size(), result.getResults().size());

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

                return context.getAttempt() < attempts ? context.retry() : context.fail();
            }).aggregate("test"));

            assertFalse(result.isSuccess());
            assertTrue(result.getResults().isEmpty());
            assertEquals(channels.size(), result.getErrors().size());

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
            assertEquals(channels.size() - attempts, result.getResults().size());
            assertEquals(attempts, result.getErrors().size());

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
            assertTrue(result.getResults().isEmpty());
            assertEquals(channels.size(), result.getErrors().size());
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
                            .filter(c -> c.getNodeId().equals(context.getFailedNode().getId()))
                            .findFirst()
                            .orElseThrow(AssertionError::new)
                            .leave();

                        leaveRef.set(leave);
                    } catch (HekateFutureException | InterruptedException e) {
                        errRef.compareAndSet(null, e);
                    }

                    return context.retry().withRoutingPolicy(policy);
                }).aggregate("test"));

                if (errRef.get() != null) {
                    throw new AssertionError(errRef.get());
                }

                assertNotNull(leaveRef.get());
                assertFalse(result.toString(), result.isSuccess());
                assertEquals(result.toString(), 1, result.getErrors().size());
                assertEquals(result.toString(), channels.size() - 2, result.getResults().size());

                Throwable expected = result.getErrors().get(leaveRef.get().getInstance().getNode());

                assertNotNull(result.toString(), expected);

                leaveRef.get().join();

                awaitForChannelsTopology(channels);
            });
        }
    }
}
