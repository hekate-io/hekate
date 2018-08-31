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
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseCallbackAcceptTest extends MessagingServiceTestBase {
    private final AtomicInteger failures = new AtomicInteger();

    private TestChannel sender;

    private TestChannel receiver;

    public ResponseCallbackAcceptTest(MessagingTestContext ctx) {
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
                msg.reply(msg.get() + "-reply");
            }
        }));

        awaitForChannelsTopology(channels);

        sender = channels.get(0);

        receiver = channels.get(1);
    }

    @Test
    public void testCompleteNoPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.endpoint().remoteNodeId());

                if (err != null) {
                    return ReplyDecision.COMPLETE;
                }

                accepts.incrementAndGet();

                return ReplyDecision.COMPLETE;
            }
        };

        sender.get().forNode(receiver.nodeId()).request("test", callback);

        assertEquals("test-reply", callback.get().get());
        assertEquals(1, accepts.get());
    }

    @Test
    public void testCompleteWithPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.from().id());

                accepts.incrementAndGet();

                return ReplyDecision.COMPLETE;
            }
        };

        sender.get()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test", callback);

        assertEquals("test-reply", callback.get().get());
        assertEquals(1, accepts.get());
    }

    @Test
    public void testRetryNoPolicy() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.from().id());

                if (err != null) {
                    return ReplyDecision.COMPLETE;
                }

                accepts.incrementAndGet();

                return ReplyDecision.REJECT;
            }
        };

        sender.get().forNode(receiver.nodeId()).request("test", callback);

        assertEquals("test-reply", callback.get().get());
        assertEquals(1, accepts.get());
        assertEquals(1, receiver.received().size());
    }

    @Test
    public void testRetryWithPolicyRetry() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.from().id());

                if (err != null) {
                    return ReplyDecision.COMPLETE;
                }

                accepts.incrementAndGet();

                return accepts.get() == 3 ? ReplyDecision.COMPLETE : ReplyDecision.REJECT;
            }
        };

        sender.get()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test", callback);

        assertEquals("test-reply", callback.get().get());
        assertEquals(3, accepts.get());
        assertEquals(3, receiver.received().size());
    }

    @Test
    public void testRetryWithPolicyFail() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.from().id());

                if (err != null) {
                    return ReplyDecision.COMPLETE;
                }

                accepts.incrementAndGet();

                return ReplyDecision.REJECT;
            }
        };

        sender.get()
            .withFailover(FailoverContext::fail)
            .forNode(receiver.nodeId())
            .request("test", callback);

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

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                if (err == null) {
                    assertEquals(receiver.nodeId(), reply.from().id());
                }

                accepts.incrementAndGet();

                return ReplyDecision.DEFAULT;
            }
        };

        sender.get()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test", callback);

        callback.get();

        assertEquals(2, accepts.get());
        assertEquals(2, receiver.received().size());
    }

    @Test
    public void testRetryWithError() throws Exception {
        failures.set(1);

        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                if (err == null) {
                    assertEquals(receiver.nodeId(), reply.from().id());
                }

                accepts.incrementAndGet();

                return err == null ? ReplyDecision.COMPLETE : ReplyDecision.REJECT;
            }
        };

        sender.get()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test", callback);

        callback.get();

        assertEquals(2, accepts.get());
        assertEquals(2, receiver.received().size());
    }

    @Test
    public void testCompleteAfterReject() throws Exception {
        AtomicInteger accepts = new AtomicInteger();

        ResponseCallbackMock callback = new ResponseCallbackMock("test") {
            @Override
            public ReplyDecision accept(Throwable err, Response<String> reply) {
                assertEquals(receiver.nodeId(), reply.from().id());

                if (err != null) {
                    return ReplyDecision.COMPLETE;
                }

                if (accepts.incrementAndGet() == 3) {
                    return ReplyDecision.COMPLETE;
                } else {
                    return ReplyDecision.REJECT;
                }
            }
        };

        sender.get()
            .withFailover(FailoverContext::retry)
            .forNode(receiver.nodeId())
            .request("test", callback);

        callback.get();

        assertEquals(3, accepts.get());
        assertEquals(3, receiver.received().size());
    }
}
