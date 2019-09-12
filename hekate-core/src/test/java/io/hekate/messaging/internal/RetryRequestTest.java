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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RetryRequestTest extends RetryTestBase {
    public RetryRequestTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testChannelCloseWhileRetrying() throws Exception {
        failures.set(Integer.MAX_VALUE);

        try {
            toRemote.newRequest("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .onRetry(err ->
                        sender.node().leaveAsync()
                    )
                )
                .response();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingException);
            assertEquals("Channel closed [channel=test-channel]", e.getCause().getMessage());
        }
    }

    @Test
    public void testRoutingPolicyReRoute() throws Exception {
        RetryRoutingPolicy policy = RetryRoutingPolicy.RE_ROUTE;

        List<FailedAttempt> attempts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        for (int i = 0; i < attempts.size(); i++) {
            FailedAttempt ctx = attempts.get(i);

            if (i % 2 == 0) {
                assertEquals(receiver.nodeId(), ctx.lastTriedNode().id());
            } else {
                assertEquals(sender.nodeId(), ctx.lastTriedNode().id());
            }
        }
    }

    @Test
    public void testRoutingPolicyPreferSameNode() throws Exception {
        RetryRoutingPolicy policy = RetryRoutingPolicy.PREFER_SAME_NODE;

        List<FailedAttempt> attempts = testWithRoutingPolicy(policy, details -> {
            if (details.isFirstAttempt()) {
                receiver.node().leaveAsync().join();

                sender.awaitForTopology(Collections.singletonList(sender));
            }
        });

        for (int i = 0; i < attempts.size(); i++) {
            FailedAttempt ctx = attempts.get(i);

            if (i == 0) {
                assertEquals(receiver.nodeId(), ctx.lastTriedNode().id());
            } else {
                assertEquals(sender.nodeId(), ctx.lastTriedNode().id());
            }
        }
    }

    @Test
    public void testRoutingPolicyRetrySameNode() throws Exception {
        RetryRoutingPolicy policy = RetryRoutingPolicy.RETRY_SAME_NODE;

        List<FailedAttempt> attempts = testWithRoutingPolicy(policy, details -> { /* No-op.*/ });

        attempts.forEach(ctx -> assertEquals(receiver.nodeId(), ctx.lastTriedNode().id()));
    }

    @Test
    public void testRetryRemoteSuccess() throws Exception {
        doTestRetrySuccess(toRemote);
    }

    @Test
    public void testRetrySelfFSuccess() throws Exception {
        doTestRetrySuccess(toSelf);
    }

    @Test
    public void testRemoteRetryDelay() throws Exception {
        doTestRetryDelay(toRemote);
    }

    @Test
    public void testSelfRetryDelay() throws Exception {
        doTestRetryDelay(toSelf);
    }

    @Test
    public void testRetryRemoteFailure() throws Exception {
        doTestRetryFailure(toRemote, MessagingRemoteException.class);
    }

    @Test
    public void testRetrySelfFailure() throws Exception {
        doTestRetryFailure(toSelf, MessagingRemoteException.class);
    }

    @Test
    public void testNoRetryOfRoutingErrors() throws Exception {
        AtomicInteger retries = new AtomicInteger();

        ClusterNodeId unknown = newNodeId();

        try {
            sender.channel().forNode(unknown)
                .newRequest("error")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .alwaysReRoute()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .response();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(EmptyTopologyException.class));
        }

        assertEquals(0, retries.get());
    }

    private List<FailedAttempt> testWithRoutingPolicy(RetryRoutingPolicy policy, Consumer<FailedAttempt> onRetry)
        throws Exception {
        List<FailedAttempt> attempts = synchronizedList(new ArrayList<>());

        failures.set(5);

        AtomicReference<TestChannel> lastTried = new AtomicReference<>();

        String response = sender.channel()
            .withLoadBalancer((message, ctx) -> {
                if (lastTried.get() == null || lastTried.get() == sender) {
                    lastTried.set(receiver);
                } else {
                    lastTried.set(sender);
                }

                return lastTried.get().nodeId();
            })
            .newRequest("test")
            .withRetry(retry -> retry
                .route(policy)
                .unlimitedAttempts()
                .onRetry(err -> {
                    attempts.add(err);

                    onRetry.accept(err);
                })
            )
            .response();

        assertEquals(5, attempts.size());
        assertEquals("test-" + RETRANSMIT_SUFFIX, response);

        return attempts;
    }

    private void doTestRetrySuccess(MessagingChannel<String> channel) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            String response = channel.newRequest("test")
                .withRetry(retry -> retry
                    .maxAttempts(attempts)
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .response();

            assertNotNull(response);
            assertEquals("test-" + RETRANSMIT_SUFFIX, response);

            assertEquals(attempts, retries.get());
        });
    }

    private void doTestRetryDelay(MessagingChannel<String> channel) throws Exception {
        failures.set(3);

        List<Long> times = synchronizedList(new ArrayList<>());

        String response = channel.newRequest("test")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .onRetry(err -> {
                    long time = System.nanoTime();

                    times.add(time);
                })
            )
            .response();

        assertNotNull(response);

        assertEquals(3, times.size());
        assertEquals("test-" + RETRANSMIT_SUFFIX, response);

        long prevTime = 0;

        for (Long time : times) {
            if (prevTime != 0) {
                assertTrue(time - prevTime >= TimeUnit.MILLISECONDS.toNanos(TEST_BACKOFF_DELAY));
            }

            prevTime = time;
        }
    }

    private void doTestRetryFailure(MessagingChannel<String> channel, Class<? extends Throwable> errorType) throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(Integer.MAX_VALUE);

            AtomicInteger retries = new AtomicInteger();

            try {
                channel.newRequest("test")
                    .withRetry(retry -> retry
                        .unlimitedAttempts()
                        .maxAttempts(attempts)
                        .onRetry(err ->
                            retries.incrementAndGet()
                        )
                    )
                    .get();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(errorType));
            }

            assertEquals(attempts, retries.get());
        });
    }
}
