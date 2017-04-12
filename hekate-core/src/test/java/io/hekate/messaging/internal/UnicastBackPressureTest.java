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
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendFuture;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnicastBackPressureTest extends BackPressureTestBase {
    public UnicastBackPressureTest(BackPressureTestContext ctx) {
        super(ctx);
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
        List<ResponseFuture<String>> futureResponses = requestUpToHighWatermark(sender);

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        assertBackPressureEnabled(sender);

        // Go down to low watermark.
        requests.stream().limit(getLowWatermarkBounds()).forEach(r -> r.reply("ok"));

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        get(sender.send("last"));

        requests.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            get(future);
        }
    }

    @Test
    public void testRequestWithTimeout() throws Exception {
        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        MessagingChannel<String> sender = createChannel(c -> {
            useBackPressure(c);
            c.getBackPressure().setOutOverflow(MessagingOverflowPolicy.BLOCK);
        }).join().get().forRemotes();

        // Enforce back pressure on sender.
        List<ResponseFuture<String>> futureResponses = requestUpToHighWatermark(sender);

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        // Check timeout.
        try {
            get(sender.withTimeout(10, MILLISECONDS).request("timeout"));

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(MessageQueueTimeoutException.class));
        }

        // Go down to low watermark.
        requests.stream().limit(getLowWatermarkBounds()).forEach(r -> r.reply("ok"));

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        get(sender.send("last"));

        requests.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            get(future);
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
        List<ResponseFuture<String>> futureResponses = requestUpToHighWatermark(senderChannel);

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
        get(senderChannel.send("last"));
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
        Future<ResponseFuture<String>> async = runAsync(() ->
            sender.get().forRemotes().request("must-block")
        );

        // Give async thread some time to block.
        expect(TimeoutException.class, () -> async.get(200, TimeUnit.MILLISECONDS));

        say("Stopping");

        // Stop sender.
        sender.leave();

        say("Stopped");

        // Await for last send operation to be unblocked.
        ResponseFuture<String> request = get(async);

        // Check that last send operation failed.
        try {
            get(request);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(Utils.getStackTrace(e), e.isCausedBy(MessagingChannelClosedException.class));
        }
    }

    @Test
    public void testSend() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg ->
            await(resumeReceive, 10)
        )).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that sender -> receiver connection is established.
        get(sender.send("init"));

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
            assertTrue(e.toString(), e.isCausedBy(MessageQueueOverflowException.class));
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
        List<ResponseFuture<String>> futureResponses = requestUpToHighWatermark(receiver.get());

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        // Send trigger message sender -> receiver.
        sender.get().forRemotes().streamRequest("init");

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

        received.partialReply("last", errFuture::complete);

        assertNull(get(errFuture));
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
        ResponseFuture<String> request = second.get().forRemotes().request("check");

        // Make sure that there were no back pressure-related errors.
        assertNull(get(errFuture));

        assertEquals("ok", get(request).get());
    }

    protected void assertBackPressureOnPartialReply(Message<String> msg) throws Exception {
        assertTrue(msg.mustReply());

        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        msg.partialReply("fail-on-back-pressure", errFuture::complete);

        Throwable err = get(errFuture);

        assertNotNull(err);
        assertTrue(err.toString(), Utils.isCausedBy(err, MessageQueueOverflowException.class));
    }

    protected List<ResponseFuture<String>> requestUpToHighWatermark(MessagingChannel<String> channel) {
        List<ResponseFuture<String>> responses = new ArrayList<>();

        // Request up to high watermark in order to enable back pressure.
        for (int i = 0; i < highWatermark; i++) {
            responses.add(channel.forRemotes().request("request-" + i));
        }

        return responses;
    }
}
