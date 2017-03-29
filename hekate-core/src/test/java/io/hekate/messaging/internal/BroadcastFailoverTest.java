package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.broadcast.AggregateResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BroadcastFailoverTest extends MessagingServiceTestBase {
    private AtomicInteger failures = new AtomicInteger();

    private List<TestChannel> channels;

    private TestChannel sender;

    public BroadcastFailoverTest(MessagingTestContext ctx) {
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
    public void testFailoverSuccess() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(channels.size() * attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).aggregate("test").get(3, TimeUnit.SECONDS);

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.getReplies().size());

            assertEquals(channels.size() * attempts, failoverCalls.get());
        });
    }

    @Test
    public void testFailoverDelay() throws Exception {
        int failoverDelay = 200;

        failures.set(channels.size() * 3);

        Map<ClusterNodeId, List<Long>> times = new HashMap<>();

        channels.forEach(c -> times.put(c.getNodeId(), Collections.synchronizedList(new ArrayList<>())));

        AggregateResult<String> result = sender.get().withFailover(ctx -> {
            times.get(ctx.getLastNode().orElseThrow(AssertionError::new).getId()).add(System.currentTimeMillis());

            return ctx.retry().withDelay(failoverDelay);
        }).aggregate("test").get(3, TimeUnit.SECONDS);

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
        assertEquals(channels.size(), result.getReplies().size());
    }

    @Test
    public void testPartialFailoverSuccess() throws Exception {
        repeat(2, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.retry();
            }).aggregate("test").get(3, TimeUnit.SECONDS);

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.getReplies().size());

            assertEquals(attempts, failoverCalls.get());
        });
    }

    @Test
    public void testFailoverFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.getAttempt() < attempts ? context.retry() : context.fail();
            }).aggregate("test").get(3, TimeUnit.SECONDS);

            assertFalse(result.isSuccess());
            assertTrue(result.getReplies().isEmpty());
            assertEquals(channels.size(), result.getErrors().size());

            assertEquals(channels.size() * attempts + channels.size() /* <- we count last failed attempts too. */, failoverCalls.get());
        });
    }

    @Test
    public void testPartialFailoverFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger failoverCalls = new AtomicInteger();

            AggregateResult<String> result = sender.get().withFailover(context -> {
                failoverCalls.incrementAndGet();

                return context.fail();
            }).aggregate("test").get(3, TimeUnit.SECONDS);

            assertFalse(result.isSuccess());
            assertEquals(channels.size() - attempts, result.getReplies().size());
            assertEquals(attempts, result.getErrors().size());

            assertEquals(attempts, failoverCalls.get());
        });
    }

    @Test
    public void testErrorInFailoverPolicy() throws Exception {
        repeat(3, i -> {
            failures.set(Integer.MAX_VALUE);

            AggregateResult<String> result = sender.get().withFailover(context -> {
                throw TEST_ERROR;
            }).aggregate("test").get(3, TimeUnit.SECONDS);

            assertFalse(result.isSuccess());
            assertTrue(result.getReplies().isEmpty());
            assertEquals(channels.size(), result.getErrors().size());
        });
    }
}
