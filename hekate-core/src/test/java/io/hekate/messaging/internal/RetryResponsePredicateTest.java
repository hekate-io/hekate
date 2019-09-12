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

import io.hekate.messaging.retry.RetryResponsePredicate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RetryResponsePredicateTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private TestChannel sender;

    private TestChannel receiver;

    public RetryResponsePredicateTest(MessagingTestContext ctx) {
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
    public void testAccept() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RetryResponsePredicate<String> predicate = rsp -> {
            assertEquals(receiver.nodeId(), rsp.endpoint().remoteNodeId());

            accepts.incrementAndGet();

            return false;
        };

        sender.channel().forNode(receiver.nodeId())
            .newRequest("test")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .whileResponse(predicate)
            )
            .submit(callback);

        assertEquals("test-reply", callback.get().payload());
        assertEquals(1, accepts.get());
    }

    @Test
    public void testAcceptWithError() throws Exception {
        failures.set(1);

        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RetryResponsePredicate<String> predicate = rsp -> {
            assertEquals(receiver.nodeId(), rsp.from().id());

            accepts.incrementAndGet();

            return false;
        };

        sender.channel().forNode(receiver.nodeId())
            .newRequest("test")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .whileResponse(predicate)
            )
            .submit(callback);

        callback.get();

        assertEquals(1, accepts.get());
        assertEquals(2, receiver.received().size());
    }

    @Test
    public void testRetry() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RetryResponsePredicate<String> predicate = rsp -> {
            assertEquals(receiver.nodeId(), rsp.from().id());

            return accepts.incrementAndGet() != 3;
        };

        sender.channel().forNode(receiver.nodeId())
            .newRequest("test")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .whileResponse(predicate)
            )
            .submit(callback);

        callback.get();

        assertEquals(3, accepts.get());
        assertEquals(3, receiver.received().size());
    }

    @Test
    public void testRetryWithError() throws Exception {
        failures.set(1);

        AtomicInteger accepts = new AtomicInteger();

        RequestCallbackMock callback = new RequestCallbackMock("test");

        RetryResponsePredicate<String> predicate = rsp -> {
            assertEquals(receiver.nodeId(), rsp.from().id());

            accepts.incrementAndGet();

            return false;
        };

        sender.channel().forNode(receiver.nodeId())
            .newRequest("test")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .whileResponse(predicate)
            )
            .submit(callback);

        callback.get();

        assertEquals(1, accepts.get());
        assertEquals(2, receiver.received().size());
    }
}
