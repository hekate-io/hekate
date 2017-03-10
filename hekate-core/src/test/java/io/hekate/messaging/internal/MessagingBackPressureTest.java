/*
 * Copyright 2017 The Hekate Project
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

import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingOverflowException;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingBackPressureTest extends MessagingServiceTestBase {
    public static class BackPressureTestContext {
        private final MessagingTestContext parent;

        private final int lowWatermark;

        private final int highWatermark;

        public BackPressureTestContext(MessagingTestContext parent, int lowWatermark, int highWatermark) {
            this.parent = parent;
            this.lowWatermark = lowWatermark;
            this.highWatermark = highWatermark;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    protected final int lowWatermark;

    protected final int highWatermark;

    public MessagingBackPressureTest(BackPressureTestContext ctx) {
        super(ctx.parent);

        lowWatermark = ctx.lowWatermark;
        highWatermark = ctx.highWatermark;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<BackPressureTestContext> getBackPressureTestContexts() {
        return getMessagingServiceTestContexts().stream().flatMap(ctx ->
            Stream.of(
                new BackPressureTestContext(ctx, 0, 1),
                new BackPressureTestContext(ctx, 2, 4)
            ))
            .collect(toList());
    }

    @Test
    public void testRequest() throws Exception {
        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Enforce back pressure on sender.
        List<RequestFuture<String>> futureResponses = requestUpToHighWatermark(sender);

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        assertBackPressureEnabled(sender);

        // Go down to low watermark.
        requests.stream().limit(getLowWatermarkBounds()).forEach(r -> r.reply("ok"));

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        sender.send("last").get(3, TimeUnit.SECONDS);

        requests.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            future.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRequestFailureWithReceiverStop() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();

        TestChannel receiver1 = createChannel(c -> useBackPressure(c)
            .withReceiver(requests1::add)
        ).join();

        TestChannel sender = createChannel(this::useBackPressure).join();

        MessagingChannel<String> senderChannel = sender.get().forRemotes();

        // Enforce back pressure on sender.
        List<RequestFuture<String>> futureResponses = requestUpToHighWatermark(senderChannel);

        busyWait("requests received", () -> requests1.size() == highWatermark);

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(senderChannel);

        // Stop receiver so that all pending requests would fail and back pressure would be unblocked.
        receiver1.leave();

        TestChannel receiver2 = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // No-op.
            })
        ).join();

        awaitForChannelsTopology(sender, receiver2);

        busyWait("all responses failed", () -> futureResponses.stream().allMatch(CompletableFuture::isDone));

        // Check that new request can be processed.
        senderChannel.send("last").get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testRequestBlockWithSenderStop() throws Exception {
        AtomicInteger requests = new AtomicInteger();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> requests.incrementAndGet())
        ).join();

        TestChannel sender = createChannel(c ->
            c.withBackPressure(bp -> {
                // Use blocking policy.
                bp.setOutLowWatermark(0);
                bp.setOutHighWatermark(1);
                bp.setOutOverflow(MessagingOverflowPolicy.BLOCK);
            })
        ).join();

        // Request up to high watermark.
        sender.get().forRemotes().request("request");

        busyWait("requests received", () -> requests.get() == 1);

        // Asynchronously try to send a new message (should block).
        Future<RequestFuture<String>> async = runAsync(() ->
            sender.get().forRemotes().request("must-block")
        );

        // Give async thread some time to block.
        expect(TimeoutException.class, () -> async.get(200, TimeUnit.MILLISECONDS));

        say("Stopping");

        // Stop sender.
        sender.leave();

        say("Stopped");

        // Await for last send operation to be unblocked.
        RequestFuture<String> request = async.get(3, TimeUnit.SECONDS);

        // Check that last send operation failed.
        try {
            request.get(3, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(Utils.getStacktrace(e), e.isCausedBy(MessagingChannelClosedException.class));
        }
    }

    @Test
    public void testSend() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg ->
            await(resumeReceive)
        )).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that sender -> receiver connection is established.
        sender.send("init").get(3, TimeUnit.SECONDS);

        SendFuture future = null;

        try {
            for (int step = 0; step < 200000; step++) {
                future = sender.send("message-" + step);

                if (future.isCompletedExceptionally()) {
                    say("Completed after " + step + " steps.");

                    break;
                }
            }
        } finally {
            resumeReceive.countDown();
        }

        assertNotNull(future);
        assertTrue(future.isCompletedExceptionally());

        try {
            future.get();

            fail("Error was expected");
        } catch (MessagingFutureException e) {
            assertTrue(e.toString(), e.isCausedBy(MessagingOverflowException.class));
        }
    }

    @Test
    public void testBroadcast() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.get().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.get().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        BroadcastFuture<String> future = null;

        try {
            for (int step = 0; step < 200000; step++) {
                future = sender.broadcast("message-" + step);

                if (future.isCompletedExceptionally()) {
                    say("Completed after " + step + " steps.");

                    break;
                }
            }
        } finally {
            resumeReceive.countDown();
        }

        assertNotNull(future);
        assertTrue(future.isCompletedExceptionally());

        try {
            future.get();

            fail("Error was expected");
        } catch (MessagingFutureException e) {
            assertTrue(e.toString(), e.isCausedBy(MessagingOverflowException.class));
        }
    }

    @Test
    public void testAggregate() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests1.add(msg);
                }
            })
        ).join();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests2.add(msg);
                }
            })
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark; i++) {
            futureResponses.add(sender.aggregate("request-" + i));
        }

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        // Go down to low watermark.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests2.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        sender.broadcast("last").get(3, TimeUnit.SECONDS);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));
        requests2.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            future.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAggregateFailure() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests1.add(msg);
                }
            })
        ).join();

        TestChannel receiver2 = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests2.add(msg);
                }
            })
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark; i++) {
            futureResponses.add(sender.aggregate("request-" + i));
        }

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        // Go down to low watermark on first node.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        // Stop second receiver so that all pending requests would partially fail.
        receiver2.leave();

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        sender.broadcast("last").get(3, TimeUnit.SECONDS);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            future.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPartialReplyIsGuarded() throws Exception {
        AtomicReference<Message<String>> receivedRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> useBackPressure(c)
            .withReceiver(receivedRef::set)
        ).join();

        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        TestChannel sender = createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        awaitForChannelsTopology(sender, receiver);

        // Enforce back pressure on receiver in order to block sending of partial responses.
        List<RequestFuture<String>> futureResponses = requestUpToHighWatermark(receiver.get());

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        // Send trigger message sender -> receiver.
        sender.get().forRemotes().request("init");

        // Await for trigger message to be received.
        busyWait("trigger received", () -> receivedRef.get() != null);

        Message<String> received = receivedRef.get();

        // Make sure that back pressure is applied when sending partial reply.
        assertBackPressureOnPartialReply(received);

        // Go down to low watermark.
        requests.stream().limit(getLowWatermarkBounds()).forEach(r -> r.reply("ok"));

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that partial replies can be send.
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        received.replyPartial("last", errFuture::complete);

        assertNull(errFuture.get(3, TimeUnit.SECONDS));
    }

    @Test
    public void testReplyIsNotGuarded() throws Exception {
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        TestChannel first = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // This message should be received when this channel's back pressure is enabled.
                if (msg.mustReply() && msg.get().equals("check")) {
                    msg.reply("ok", errFuture::complete);
                }
            })
        ).join();

        TestChannel second = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // Ignore all messages since we need to reach the back pressure limits on the 'first' node by not sending responses.
            })
        ).join();

        awaitForChannelsTopology(first, second);

        // Enforce back pressure on 'first'.
        requestUpToHighWatermark(first.get());

        // 'first' must have back pressure being enabled.
        assertBackPressureEnabled(first.get());

        // Request 'second' -> 'first' in order to trigger response while back pressure is enabled.
        RequestFuture<String> request = second.get().forRemotes().request("check");

        // Make sure that there were no back pressure-related errors.
        assertNull(errFuture.get(3, TimeUnit.SECONDS));

        assertEquals("ok", request.get(3, TimeUnit.SECONDS).get());
    }

    @Test
    public void testConversationIsNotGuarded() throws Exception {
        CompletableFuture<Throwable> conversationErrFuture = new CompletableFuture<>();

        TestChannel first = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // This message should be received when this channel's back pressure is enabled.
                if (msg.mustReply() && msg.get().equals("check")) {
                    // Send back the conversation request.
                    msg.replyWithRequest("reply-request", (err, reply) ->
                        conversationErrFuture.complete(err)
                    );
                }
            })
        ).join();

        TestChannel second = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // Ignore all messages since we need to reach the back pressure limits on the 'first' node by not sending responses.
            })
        ).join();

        awaitForChannelsTopology(first, second);

        // Enforce back pressure on 'first'.
        requestUpToHighWatermark(first.get());

        // 'first' must have back pressure being enabled.
        assertBackPressureEnabled(first.get().forRemotes());

        // Request 'second' -> 'first' in order to trigger conversation while back pressure is enabled.
        second.get().forRemotes().request("check", (err, reply) -> {
            if (reply != null) {
                reply.reply("ignore");
            }
        });

        // Make sure that there were no back pressure-related errors in conversation request.
        assertNull(conversationErrFuture.get(3, TimeUnit.SECONDS));
    }

    private MessagingChannelConfig<String> useBackPressure(MessagingChannelConfig<String> cfg) {
        return cfg.withBackPressure(bp -> {
            bp.setOutOverflow(MessagingOverflowPolicy.FAIL);
            bp.setOutLowWatermark(lowWatermark);
            bp.setOutHighWatermark(highWatermark);
            bp.setInLowWatermark(lowWatermark);
            bp.setInHighWatermark(highWatermark);
        });
    }

    private void assertBackPressureEnabled(MessagingChannel<String> channel) {
        // Check that message can't be sent when high watermark reached.
        try {
            channel.request("fail-on-back-pressure").get(3, TimeUnit.SECONDS);

            fail("Back pressure error was expected [channel=" + channel + ']');
        } catch (TimeoutException | InterruptedException e) {
            throw new AssertionError(e);
        } catch (MessagingFutureException e) {
            assertTrue(e.toString(), e.isCausedBy(MessagingOverflowException.class));
        }
    }

    private void assertBackPressureOnPartialReply(Message<String> msg) throws Exception {
        assertTrue(msg.mustReply());

        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        msg.replyPartial("fail-on-back-pressure", errFuture::complete);

        Throwable err = errFuture.get(3, TimeUnit.SECONDS);

        assertNotNull(err);
        assertTrue(err.toString(), Utils.isCausedBy(err, MessagingOverflowException.class));
    }

    private List<RequestFuture<String>> requestUpToHighWatermark(MessagingChannel<String> channel) {
        List<RequestFuture<String>> responses = new ArrayList<>();

        // Request up to high watermark in order to enable back pressure.
        for (int i = 0; i < highWatermark; i++) {
            responses.add(channel.forRemotes().request("request-" + i));
        }

        return responses;
    }

    private int getLowWatermarkBounds() {
        return Math.max(1, lowWatermark);
    }
}
