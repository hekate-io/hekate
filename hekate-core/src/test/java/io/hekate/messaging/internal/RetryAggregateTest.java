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

import io.hekate.messaging.operation.AggregateResult;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RetryAggregateTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private List<TestChannel> channels;

    private TestChannel sender;

    public RetryAggregateTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        channels = createAndJoinChannels(3, c -> c.withReceiver(msg -> {
            if (failures.getAndDecrement() > 0) {
                throw TEST_ERROR;
            }

            msg.reply(msg.payload() + "reply");
        }));

        sender = channels.get(0);
    }

    @Test
    public void testSuccess() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(channels.size() * attempts);

            AtomicInteger retries = new AtomicInteger();

            AggregateResult<String> result = get(sender.channel()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit()
            );

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            assertEquals(channels.size() * attempts, retries.get());
        });
    }

    @Test
    public void testPartialSuccess() throws Exception {
        repeat(2, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            AggregateResult<String> result = get(sender.channel()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .maxAttempts(attempts)
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit()
            );

            assertTrue(result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger retries = new AtomicInteger();

            AggregateResult<String> result = get(sender.channel()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .maxAttempts(attempts)
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit()
            );

            assertFalse(result.isSuccess());
            assertTrue(result.results().isEmpty());
            assertEquals(channels.size(), result.errors().size());

            assertEquals(channels.size() * attempts, retries.get());
        });
    }

    @Test
    public void testPartialFailure() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            AggregateResult<String> result = get(sender.channel()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .whileError(err -> {
                        retries.incrementAndGet();

                        return false;
                    })
                )
                .submit()
            );

            assertFalse(result.isSuccess());
            assertEquals(channels.size() - attempts, result.results().size());
            assertEquals(attempts, result.errors().size());

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testErrorInOnRetryCallback() throws Exception {
        repeat(3, i -> {
            failures.set(Integer.MAX_VALUE);

            AggregateResult<String> result = get(sender.channel()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .onRetry(err -> {
                        throw TEST_ERROR;
                    })
                )
                .submit()
            );

            assertFalse(result.isSuccess());
            assertTrue(result.results().isEmpty());
            assertEquals(channels.size(), result.errors().size());
        });
    }

    @Test
    public void testNodeLeaveDuringRetry() throws Exception {
        repeat(3, i -> {
            failures.set(1);

            AtomicReference<Exception> errRef = new AtomicReference<>();
            AtomicReference<TestChannel> leaveRef = new AtomicReference<>();

            AggregateResult<String> result = sender.channel().forRemotes()
                .newAggregate("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .onRetry(err -> {
                        try {
                            TestChannel leave = channels.stream()
                                .filter(c -> c.nodeId().equals(err.lastTriedNode().id()))
                                .findFirst()
                                .orElseThrow(AssertionError::new)
                                .leave();

                            leaveRef.set(leave);
                        } catch (ExecutionException | InterruptedException | TimeoutException e) {
                            errRef.compareAndSet(null, e);
                        }
                    })
                )
                .get();

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
