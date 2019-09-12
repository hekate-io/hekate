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
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.operation.RequestFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackPressureRequestTest extends BackPressureParametrizedTestBase {
    public BackPressureRequestTest(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().channel().forRemotes();

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
        get(sender.newSend("last").submit());

        requests.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            get(future);
        }
    }

    @Test
    public void testContinuousBlocking() throws Exception {
        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> msg.reply("ok"))
        ).join();

        MessagingChannel<String> sender = createChannel(cfg ->
            useBackPressure(cfg).getBackPressure().withOutOverflowPolicy(MessagingOverflowPolicy.BLOCK)
        ).join().channel().forRemotes();

        int requests = 1000;

        List<RequestFuture<String>> asyncResponses = new ArrayList<>(requests);

        for (int i = 0; i < requests; i++) {
            if (i > 0 && i % 100 == 0) {
                say("Submitted requests: %s", i);
            }

            asyncResponses.add(sender.newRequest("test-" + i).submit());
        }

        for (RequestFuture<String> future : asyncResponses) {
            get(future);
        }
    }

    @Test
    public void testTimeout() throws Exception {
        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        MessagingChannel<String> sender = createChannel(c ->
            useBackPressure(c).getBackPressure().setOutOverflowPolicy(MessagingOverflowPolicy.BLOCK)
        ).join().channel().forRemotes();

        // Enforce back pressure on sender.
        List<RequestFuture<String>> futureResponses = requestUpToHighWatermark(sender);

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        // Check timeout.
        try {
            sender.newRequest("timeout").withTimeout(10, MILLISECONDS).response();

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
        get(sender.newSend("last").submit());

        requests.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            get(future);
        }
    }

    @Test
    public void testFailWithReceiverStop() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();

        TestChannel receiver1 = createChannel(c -> useBackPressure(c)
            .withReceiver(requests1::add)
        ).join();

        TestChannel sender = createChannel(this::useBackPressure).join();

        MessagingChannel<String> senderChannel = sender.channel().forRemotes();

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
        get(senderChannel.newSend("last").submit());
    }

    @Test
    public void testBlockWithSenderStop() throws Exception {
        AtomicInteger requests = new AtomicInteger();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> requests.incrementAndGet())
        ).join();

        TestChannel sender = createChannel(c ->
            c.withBackPressure(bp -> {
                // Use blocking policy.
                bp.setOutLowWatermark(0);
                bp.setOutHighWatermark(1);
                bp.setOutOverflowPolicy(MessagingOverflowPolicy.BLOCK);
            })
        ).join();

        // Request up to high watermark.
        sender.channel().forRemotes().newRequest("request").submit();

        busyWait("requests received", () -> requests.get() == 1);

        // Asynchronously try to send a new message (should block).
        Future<RequestFuture<String>> async = runAsync(() ->
            sender.channel().forRemotes().newRequest("must-block").submit()
        );

        // Give async thread some time to block.
        expect(TimeoutException.class, () -> async.get(200, TimeUnit.MILLISECONDS));

        say("Stopping");

        // Stop sender.
        sender.leave();

        say("Stopped");

        // Await for last send operation to be unblocked.
        RequestFuture<String> request = get(async);

        // Check that last send operation failed.
        try {
            get(request);

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(ErrorUtils.stackTrace(e), e.isCausedBy(MessagingChannelClosedException.class));
        }
    }

    @Test
    public void testReplyIsNotGuarded() throws Exception {
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        TestChannel first = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                // This message should be received when this channel's back pressure is enabled.
                if (msg.mustReply() && msg.payload().equals("check")) {
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
        requestUpToHighWatermark(first.channel());

        // 'first' must have back pressure being enabled.
        assertBackPressureEnabled(first.channel());

        // Request 'second' -> 'first' in order to trigger response while back pressure is enabled.
        RequestFuture<String> request = second.channel().forRemotes().newRequest("check").submit();

        // Make sure that there were no back pressure-related errors.
        assertNull(get(errFuture));

        assertEquals("ok", get(request).payload());
    }
}
