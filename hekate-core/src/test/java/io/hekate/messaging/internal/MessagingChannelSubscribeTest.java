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

import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.operation.SendCallback;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagingChannelSubscribeTest extends MessagingServiceTestBase {
    public MessagingChannelSubscribeTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testFuture() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());

            for (int i = 0; i < 3; i++) {
                msg.partialReply("response" + i);

                assertTrue(msg.mustReply());
            }

            msg.reply("final");

            assertResponded(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            List<String> results = sender.channel().forNode(receiver.nodeId())
                .newSubscribe("request")
                .responses();

            assertEquals(Arrays.asList("response0", "response1", "response2", "final"), results);

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testCallback() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());

            for (int i = 0; i < 3; i++) {
                msg.partialReply("response" + i);

                assertTrue(msg.mustReply());
            }

            msg.reply("final");

            assertResponded(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

            List<String> senderMessages = Collections.synchronizedList(new ArrayList<>());

            sender.channel().forNode(receiver.nodeId()).newSubscribe("request").submit((err, rsp) -> {
                if (err == null) {
                    try {
                        senderMessages.add(rsp.payload());

                        if (rsp.payload().equals("final")) {
                            assertTrue(rsp.isLastPart());

                            errFuture.complete(null);
                        } else {
                            assertFalse(rsp.isLastPart());
                        }
                    } catch (Throwable e) {
                        errFuture.complete(e);
                    }
                } else {
                    errFuture.complete(err);
                }
            });

            assertNull(get(errFuture));

            List<String> expectedMessages = Arrays.asList("response0", "response1", "response2", "final");

            assertEquals(expectedMessages, senderMessages);

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testPartialReplyCallback() throws Throwable {
        AtomicReference<SendCallback> replyCallbackRef = new AtomicReference<>();
        AtomicReference<SendCallback> lastReplyCallbackRef = new AtomicReference<>();

        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());
            assertTrue(msg.isSubscription());

            assertNotNull(replyCallbackRef.get());

            for (int i = 0; i < 3; i++) {
                msg.partialReply("response" + i, replyCallbackRef.get());

                assertTrue(msg.mustReply());
                assertTrue(msg.isSubscription());
            }

            msg.reply("final", lastReplyCallbackRef.get());

            assertResponded(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            CompletableFuture<Throwable> sendErrFuture = new CompletableFuture<>();
            CompletableFuture<Throwable> receiveErrFuture = new CompletableFuture<>();

            replyCallbackRef.set(err -> {
                if (err != null) {
                    receiveErrFuture.complete(err);
                }
            });

            lastReplyCallbackRef.set(receiveErrFuture::complete);

            List<String> senderMessages = Collections.synchronizedList(new ArrayList<>());

            sender.channel().forNode(receiver.nodeId()).newSubscribe("request").submit((err, rsp) -> {
                if (err == null) {
                    try {
                        senderMessages.add(rsp.payload());

                        if (rsp.payload().equals("final")) {
                            assertTrue(rsp.isLastPart());

                            sendErrFuture.complete(null);
                        } else {
                            assertFalse(rsp.isLastPart());
                        }
                    } catch (AssertionError e) {
                        sendErrFuture.complete(e);
                        receiveErrFuture.complete(null);
                    }
                } else {
                    sendErrFuture.complete(err);
                    receiveErrFuture.complete(null);
                }
            });

            assertNull(get(receiveErrFuture));
            assertNull(get(sendErrFuture));

            List<String> expectedMessages = Arrays.asList("response0", "response1", "response2", "final");

            assertEquals(expectedMessages, senderMessages);

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testNetworkDisconnectWhileReplying() throws Throwable {
        TestChannel sender = createChannel().join();

        Exchanger<Message<String>> messageExchanger = new Exchanger<>();

        TestChannel receiver = createChannel(c ->
            c.withReceiver(msg -> {
                try {
                    messageExchanger.exchange(msg);
                } catch (InterruptedException e) {
                    throw new AssertionError("Thread was unexpectedly interrupted.", e);
                }
            })
        ).join();

        awaitForChannelsTopology(sender, receiver);

        sender.channel().forNode(receiver.nodeId()).newSubscribe("test").submit((err, rsp) -> { /* Ignore. */ });

        Message<String> msg = messageExchanger.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS);

        receiver.leave();

        Exchanger<Throwable> errExchanger = new Exchanger<>();

        for (int i = 0; i < 10; i++) {
            msg.partialReply("fail", err -> {
                try {
                    errExchanger.exchange(err);
                } catch (InterruptedException e) {
                    throw new AssertionError("Thread was unexpectedly interrupted .", e);
                }
            });

            Throwable partialErr = errExchanger.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS);

            assertTrue(getStacktrace(partialErr), partialErr instanceof MessagingException);
            assertTrue(getStacktrace(partialErr), ErrorUtils.isCausedBy(ClosedChannelException.class, partialErr));
        }

        msg.reply("fail", err -> {
            try {
                errExchanger.exchange(err);
            } catch (InterruptedException e) {
                throw new AssertionError("Thread was unexpectedly interrupted .", e);
            }
        });

        Throwable err = errExchanger.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS);

        assertTrue(getStacktrace(err), err instanceof MessagingException);
        assertTrue(getStacktrace(err), ErrorUtils.isCausedBy(ClosedChannelException.class, err));
    }
}
