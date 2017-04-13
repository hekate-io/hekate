package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.SendCallback;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagingChannelStreamRequestTest extends MessagingServiceTestBase {
    public MessagingChannelStreamRequestTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testPartialReply() throws Throwable {
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

            sender.get().forNode(receiver.getNodeId()).streamRequest("request", (err, reply) -> {
                if (err == null) {
                    try {
                        senderMessages.add(reply.get());

                        if (reply.get().equals("final")) {
                            assertFalse(reply.isPartial());

                            errFuture.complete(null);
                        } else {
                            assertTrue(reply.isPartial());
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
            assertTrue(msg.isStreamRequest());

            assertNotNull(replyCallbackRef.get());

            for (int i = 0; i < 3; i++) {
                msg.partialReply("response" + i, replyCallbackRef.get());

                assertTrue(msg.mustReply());
                assertTrue(msg.isStreamRequest());
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

            sender.get().forNode(receiver.getNodeId()).streamRequest("request", (err, reply) -> {
                if (err == null) {
                    try {
                        senderMessages.add(reply.get());

                        if (reply.get().equals("final")) {
                            assertFalse(reply.isPartial());

                            sendErrFuture.complete(null);
                        } else {
                            assertTrue(reply.isPartial());
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
}
