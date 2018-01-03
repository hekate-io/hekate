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

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.unicast.StreamFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

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

        assertEquals(1, sender.get().timeout());

        repeat(5, i -> {
            hangLatchRef.set(new CountDownLatch(1));

            try {
                get(sender.get().forRemotes().request("must-fail-" + i));
            } catch (MessagingFutureException e) {
                assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                assertEquals("Messaging operation timed out [message=must-fail-" + i + ']',
                    e.findCause(MessageTimeoutException.class).getMessage());
            } finally {
                hangLatchRef.get().countDown();
            }
        });
    }

    @Test
    public void testRequestNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1000)).join();

        assertEquals(1000, sender.get().timeout());

        repeat(5, i ->
            get(sender.get().forRemotes().request("request-" + i))
        );
    }

    @Test
    public void testStream() throws Exception {
        Exchanger<Message<String>> msgRef = new Exchanger<>();

        createChannel(c -> c.withReceiver(msg -> {
            try {
                msgRef.exchange(msg);
            } catch (InterruptedException e) {
                fail(e.toString());
            }
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(150)).join();

        assertEquals(150, sender.get().timeout());

        repeat(3, i -> {
            StreamFuture<String> future = sender.get().forRemotes().stream("must-fail-" + i);

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
        createChannel().join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1)).join();

        assertEquals(1, sender.get().timeout());

        repeat(5, i -> {
            try {
                get(sender.get().forRemotes().send("must-fail-" + i));
            } catch (MessagingFutureException e) {
                assertTrue(getStacktrace(e), e.isCausedBy(MessageTimeoutException.class));
                assertEquals("Messaging operation timed out [message=must-fail-" + i + ']',
                    e.findCause(MessageTimeoutException.class).getMessage());
            }
        });
    }

    @Test
    public void testSendNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1000)).join();

        assertEquals(1000, sender.get().timeout());

        repeat(5, i ->
            get(sender.get().forRemotes().request("request-" + i))
        );
    }
}
