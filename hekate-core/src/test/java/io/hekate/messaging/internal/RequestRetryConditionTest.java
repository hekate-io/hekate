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
import io.hekate.messaging.unicast.RejectedReplyException;
import io.hekate.messaging.unicast.RequestRetryCondition;
import io.hekate.messaging.unicast.RetryDecision;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestRetryConditionTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private TestChannel sender;

    private TestChannel receiver;

    public RequestRetryConditionTest(MessagingTestContext ctx) {
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
                msg.reply(msg.payload() + "-reply");
            }
        }));

        awaitForChannelsTopology(channels);

        sender = channels.get(0);

        receiver = channels.get(1);
    }

    @Test
    public void testAcceptWithNoFailoverPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.endpoint().remoteNodeId());

            if (err != null) {
                return RetryDecision.DONE;
            }

            accepts.incrementAndGet();

            return RetryDecision.DONE;
        };

        sender.channel().forNode(receiver.nodeId()).request("test").until(condition).async(callback);

        assertEquals("test-reply", callback.get().payload());
        assertEquals(1, accepts.get());
    }

    @Test
    public void testAcceptWithFailoverPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.from().id());

            accepts.incrementAndGet();

            return RetryDecision.DONE;
        };

        sender.channel()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        assertEquals("test-reply", callback.get().payload());
        assertEquals(1, accepts.get());
    }

    @Test
    public void testRetryWithNoFailoverPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.from().id());

            if (err != null) {
                return RetryDecision.DONE;
            }

            accepts.incrementAndGet();

            return RetryDecision.RETRY;
        };

        sender.channel().forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        assertEquals("test-reply", callback.get().payload());
        assertEquals(1, accepts.get());
        assertEquals(1, receiver.received().size());
    }

    @Test
    public void testRetryWithFailoverPolicyRetry() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.from().id());

            if (err != null) {
                return RetryDecision.DONE;
            }

            accepts.incrementAndGet();

            return accepts.get() == 3 ? RetryDecision.DONE : RetryDecision.RETRY;
        };

        sender.channel()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        assertEquals("test-reply", callback.get().payload());
        assertEquals(3, accepts.get());
        assertEquals(3, receiver.received().size());
    }

    @Test
    public void testRetryWithFailoverPolicyFail() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.from().id());

            if (err != null) {
                return RetryDecision.DONE;
            }

            accepts.incrementAndGet();

            return RetryDecision.RETRY;
        };

        sender.channel()
            .withFailover(FailoverContext::fail)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        try {
            callback.get();

            fail("Error was expected.");
        } catch (RejectedReplyException e) {
            assertTrue(e.reply().isPresent());
            assertEquals("test-reply", e.reply().get());
        }

        assertEquals(1, accepts.get());
        assertEquals(1, receiver.received().size());
    }

    @Test
    public void testDefaultWithError() throws Exception {
        failures.set(1);

        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            if (err == null) {
                assertEquals(receiver.nodeId(), reply.from().id());
            }

            accepts.incrementAndGet();

            return RetryDecision.USE_DEFAULTS;
        };

        sender.channel()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        callback.get();

        assertEquals(2, accepts.get());
        assertEquals(2, receiver.received().size());
    }

    @Test
    public void testRetryWithError() throws Exception {
        failures.set(1);

        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            if (err == null) {
                assertEquals(receiver.nodeId(), reply.from().id());
            }

            accepts.incrementAndGet();

            return err == null ? RetryDecision.DONE : RetryDecision.RETRY;
        };

        sender.channel()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        callback.get();

        assertEquals(2, accepts.get());
        assertEquals(2, receiver.received().size());
    }

    @Test
    public void testAcceptAfterReject() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RequestRetryCondition<String> condition = (err, reply) -> {
            assertEquals(receiver.nodeId(), reply.from().id());

            if (err != null) {
                return RetryDecision.DONE;
            }

            if (accepts.incrementAndGet() == 3) {
                return RetryDecision.DONE;
            } else {
                return RetryDecision.RETRY;
            }
        };

        sender.channel()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test")
            .until(condition)
            .async(callback);

        callback.get();

        assertEquals(3, accepts.get());
        assertEquals(3, receiver.received().size());
    }
}
