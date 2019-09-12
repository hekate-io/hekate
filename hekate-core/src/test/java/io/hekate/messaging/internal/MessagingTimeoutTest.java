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

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.operation.RequestFuture;
import io.hekate.messaging.operation.SubscribeFuture;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingTimeoutTest extends MessagingServiceTestBase {
    public MessagingTimeoutTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequest() throws Exception {
        AtomicReference<CountDownLatch> hangLatchRef = new AtomicReference<>();

        createChannel(c -> c.withReceiver(msg -> {
            await(hangLatchRef.get());

            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1)).join();

        repeat(5, i -> {
            hangLatchRef.set(new CountDownLatch(1));

            try {
                MessagingFutureException e = expect(MessagingFutureException.class, () ->
                    get(sender.channel()
                        .forRemotes()
                        .newRequest("must-fail-" + i)
                        .submit())
                );

                assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                assertEquals(
                    "Messaging operation timed out [timeout=1, message=must-fail-" + i + ']',
                    e.findCause(MessageTimeoutException.class).getMessage()
                );
            } finally {
                hangLatchRef.get().countDown();
            }
        });
    }

    @Test
    public void testRequestTimeoutOnReceiver() throws Exception {
        // Test only if messages are processed by worker threads (NIO threads ignore timeouts anyway).
        if (workerThreads() > 0) {
            int timeout = 100;

            TestChannel sender = createChannel(c -> c.withMessagingTimeout(timeout)).join();

            repeat(3, i -> {
                CountDownLatch receiverReadyLatch = new CountDownLatch(1);
                CountDownLatch receiverHangLatch = new CountDownLatch(1);
                CountDownLatch receiverDoneLatch = new CountDownLatch(1);
                AtomicInteger receiverCalls = new AtomicInteger();

                TestChannel receiver = createChannel(c -> c.withReceiver(msg -> {
                    receiverReadyLatch.countDown();

                    await(receiverHangLatch);

                    receiverCalls.incrementAndGet();

                    msg.reply("done");

                    receiverDoneLatch.countDown();
                })).join();

                awaitForChannelsTopology(sender, receiver);

                // Submit requests.
                // Note we are using the same affinity key for all messages so that they would all go to the same thread.
                List<RequestFuture<String>> futures = Stream.of(i, i + 1, i + 2)
                    .map(req -> sender.channel().forRemotes()
                        .newRequest("must-fail-" + req)
                        .withAffinity("1")
                        .submit()
                    )
                    .collect(toList());

                // Await for the first message to be received.
                await(receiverReadyLatch);

                // Wait for a while to make sure that time out happens on the receiver side.
                sleep(timeout * 2);

                // Check results (all requests should time out).
                try {
                    for (RequestFuture<String> future : futures) {
                        MessagingFutureException e = expect(MessagingFutureException.class, () -> get(future));

                        assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                        assertTrue(e.findCause(MessageTimeoutException.class).getMessage().startsWith("Messaging operation timed out"));
                    }
                } finally {
                    // Resume the receiver thread.
                    receiverHangLatch.countDown();
                }

                // Await for the first message to be processed.
                await(receiverDoneLatch);

                // Stop receiver (ensures that all pending messages will be processed in some way).
                receiver.leave();

                // Receiver must be called only once by the first request, all other request should be skipped because of timeouts.
                assertEquals(1, receiverCalls.get());
            });
        }
    }

    @Test
    public void testRequestNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1000)).join();

        repeat(5, i ->
            get(sender.channel().forRemotes().newRequest("request-" + i).submit())
        );
    }

    @Test
    public void testSubscribe() throws Exception {
        AtomicReference<CountDownLatch> hangLatchRef = new AtomicReference<>();

        createChannel(c -> c.withReceiver(msg -> {
            await(hangLatchRef.get());

            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1)).join();

        repeat(5, i -> {
            hangLatchRef.set(new CountDownLatch(1));

            try {
                MessagingFutureException e = expect(MessagingFutureException.class, () ->
                    get(sender.channel().forRemotes().newSubscribe("must-fail-" + i).submit((err, rsp) -> { /* Ignore. */ }))
                );

                assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                assertEquals(
                    "Messaging operation timed out [timeout=1, message=must-fail-" + i + ']',
                    e.findCause(MessageTimeoutException.class).getMessage()
                );
            } finally {
                hangLatchRef.get().countDown();
            }
        });
    }

    @Test
    public void testSubscribeNoTimeout() throws Exception {
        Exchanger<Message<String>> msgRef = new Exchanger<>();

        createChannel(c -> c.withReceiver(msg -> {
            try {
                msgRef.exchange(msg);
            } catch (InterruptedException e) {
                fail(e.toString());
            }
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(150)).join();

        repeat(3, i -> {
            SubscribeFuture<String> future = sender.channel()
                .forRemotes()
                .newSubscribe("must-fail-" + i)
                .submit((err, rsp) -> { /* Ignore. */ });

            Message<String> request = msgRef.exchange(null);

            repeat(5, j -> {
                request.partialReply("part");

                sleep(50);
            });

            request.reply("final");

            get(future);
        });
    }

    @Test
    public void testSend() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        })).join();

        TestChannel sender = createChannel(c -> {
                c.withMessagingTimeout(10);
                c.withInterceptor(new ClientMessageInterceptor<String>() {
                    @Override
                    public void interceptClientSend(ClientSendContext ctx) {
                        sleep(50);
                    }
                });
            }
        ).join();

        repeat(3, i -> {
            try {
                get(sender.channel().forRemotes().newSend("must-fail-" + i).submit());

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                assertEquals("Messaging operation timed out [timeout=10, message=must-fail-" + i + ']',
                    e.findCause(MessageTimeoutException.class).getMessage());
            }
        });
    }

    @Test
    public void testSendNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            // No-op.
        })).join();

        TestChannel sender = createChannel(c -> {
                c.withMessagingTimeout(1000);
                c.withInterceptor(new ClientMessageInterceptor<String>() {
                    @Override
                    public void interceptClientSend(ClientSendContext ctx) {
                        sleep(30);
                    }
                });
            }
        ).join();

        repeat(3, i ->
            get(sender.channel().forRemotes().newSend("request-" + i).submit())
        );
    }
}
