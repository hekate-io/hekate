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
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.test.HekateTestError;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RetrySendTest extends RetryTestBase {
    public RetrySendTest(MessagingTestContext ctx) {
        super(ctx);

        spy = msg -> {
            if (msg instanceof MessagingProtocol.Notification) {
                if (failures.decrementAndGet() >= 0) {
                    throw new IOException(HekateTestError.MESSAGE);
                }
            }
        };
    }

    @Test
    public void testSuccessRetrySameNode() throws Exception {
        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            SendFuture future = sender.channel().forRemotes()
                .newSend("test")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.RETRY_SAME_NODE)
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    ))
                .submit();

            get(future);

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testSuccessReRoute() throws Exception {
        // Additional receiver for re-route.
        createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        }));

        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            SendFuture future = sender.channel().forRemotes()
                .newSend("test")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.RE_ROUTE)
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit();

            get(future);

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testSuccessPreferSameNode() throws Exception {
        // Additional receiver for re-route.
        TestChannel oneMoreNode = createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        })).join();

        awaitForChannelsTopology(sender, receiver, oneMoreNode);

        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            SendFuture future = sender.channel().forRemotes()
                .newSend("test")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.PREFER_SAME_NODE)
                    .unlimitedAttempts()
                    .onRetry(err -> {
                        try {
                            get(receiver.node().leaveAsync());
                        } catch (Exception e) {
                            fail(getStacktrace(e));
                        }

                        retries.incrementAndGet();
                    })
                )
                .submit();

            get(future);
        });
    }

    @Test
    public void testSuccessPreferSameNodeNoLeave() throws Exception {
        // Additional receiver for re-route.
        TestChannel oneMoreNode = createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        })).join();

        awaitForChannelsTopology(sender, receiver, oneMoreNode);

        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            SendFuture future = sender.channel().forRemotes()
                .newSend("test")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.PREFER_SAME_NODE)
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit();

            get(future);

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testSuccessPreferSameNodeLeave() throws Exception {
        // Additional receiver for re-route.
        createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        }));

        repeat(3, i -> {
            int attempts = i + 1;

            failures.set(attempts);

            AtomicInteger retries = new AtomicInteger();

            SendFuture future = sender.channel().forRemotes()
                .newSend("test")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.PREFER_SAME_NODE)
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit();

            get(future);

            assertEquals(attempts, retries.get());
        });
    }

    @Test
    public void testChannelCloseWhileRetrying() throws Exception {
        failures.set(Integer.MAX_VALUE);

        try {
            SendFuture future = toRemote.newSend("test")
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .onRetry(err ->
                        sender.node().leaveAsync()
                    )
                )
                .submit();

            get(future);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingException);
            assertEquals("Channel closed [channel=test-channel]", e.getCause().getMessage());
        }
    }

    @Test
    public void testNoRetryOfRoutingErrors() throws Exception {
        ClusterNodeId unknownNode = newNodeId();

        AtomicInteger retries = new AtomicInteger();

        try {
            get(sender.channel().forNode(unknownNode)
                .newSend("error")
                .withRetry(retry -> retry
                    .route(RetryRoutingPolicy.RE_ROUTE)
                    .unlimitedAttempts()
                    .onRetry(err ->
                        retries.incrementAndGet()
                    )
                )
                .submit()
            );

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(EmptyTopologyException.class));
        }

        assertEquals(0, retries.get());
    }
}
