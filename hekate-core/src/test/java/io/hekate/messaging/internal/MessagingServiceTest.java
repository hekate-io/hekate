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

import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.intercept.ServerMessageInterceptor;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.operation.SendCallback;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest extends MessagingServiceTestBase {
    private static class ExpectedSendFailure implements SendCallback {
        private final AtomicInteger sent = new AtomicInteger();

        private final CountDownLatch failureLatch = new CountDownLatch(1);

        private final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        @Override
        public void onComplete(Throwable err) {
            if (err == null) {
                sent.incrementAndGet();
            } else {
                errorRef.set(err);

                failureLatch.countDown();
            }
        }

        public void awaitAndCheck(Class<? extends Throwable> reason) throws InterruptedException {
            await(failureLatch);

            assertEquals(0, sent.get());

            Throwable error = errorRef.get();

            assertNotNull(error);

            if (!reason.isAssignableFrom(error.getClass())) {
                throw new AssertionError("Unexpected error type [expected=" + reason.getName()
                    + ", actual=" + error.getClass().getName(), error);
            }
        }
    }

    public MessagingServiceTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testChannelOptions() throws Exception {
        TestChannel channel = createChannel().join();

        assertEquals(workerThreads(), channel.channel().workerThreads());
        assertEquals(nioThreads(), channel.channel().nioThreads());
    }

    @Test
    public void testRejoin() throws Exception {
        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> msg.reply("OK"))).join();
        TestChannel sender = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            get(sender.channel().forNode(receiver.nodeId()).newRequest("success").submit());

            sender.leave();

            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.channel().forNode(receiver.nodeId()).newRequest("fail").submit())
            );

            assertTrue(err.isCausedBy(MessagingChannelClosedException.class));

            sender.join();

            get(sender.channel().forNode(receiver.nodeId()).newRequest("success").submit());
        });
    }

    @Test
    public void testRejoinSingleNode() throws Exception {
        TestChannel sender = createChannel(c -> c.withReceiver(msg -> msg.reply("ok")));

        repeat(3, i -> {
            sender.join();

            get(sender.channel().newRequest("test" + i).submit());

            sender.leave();
        });
    }

    @Test
    public void testAddRemoveNodes() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            channels.add(createChannel().join());

            awaitForChannelsTopology(channels);

            for (TestChannel from : channels) {
                for (TestChannel to : channels) {
                    from.channel().forNode(to.nodeId()).newSend("test-" + from.nodeId()).submit();
                }
            }

            for (TestChannel to : channels) {
                for (TestChannel from : channels) {
                    to.awaitForMessage("test-" + from.nodeId());
                }

                to.clearReceived();
            }
        });

        List<TestChannel> removed = new ArrayList<>();

        for (Iterator<TestChannel> it = channels.iterator(); it.hasNext(); ) {
            TestChannel channel = it.next();

            it.remove();

            removed.add(channel);

            channel.leave();

            awaitForChannelsTopology(channels);

            for (TestChannel from : channels) {
                for (TestChannel to : channels) {
                    from.channel().forNode(to.nodeId()).newSend("test-" + from.nodeId()).submit();
                }
            }

            for (TestChannel to : channels) {
                for (TestChannel from : channels) {
                    to.awaitForMessage("test-" + from.nodeId());
                }

                to.clearReceived();
            }

            for (TestChannel from : channels) {
                for (TestChannel to : removed) {
                    ExpectedSendFailure sendFailure = new ExpectedSendFailure();

                    from.channel().forNode(to.nodeId()).newSend("failed").submit(sendFailure);

                    sendFailure.awaitAndCheck(EmptyTopologyException.class);
                }
            }
        }
    }

    @Test
    public void testGlobalInterceptors() throws Exception {
        createChannel(
            c -> c.withReceiver(msg -> msg.reply(msg.payload() + "-OK")),
            boot -> boot.withService(MessagingServiceFactory.class, msg ->
                msg.withGlobalInterceptor(new ServerMessageInterceptor<Object>() {
                    @Override
                    public void interceptServerReceive(ServerReceiveContext<Object> ctx) {
                        ctx.overrideMessage(ctx.payload() + "-intercepted");
                    }
                })
            )
        ).join();

        TestChannel sender = createChannel().join();

        assertEquals("test-intercepted-OK", get(sender.channel().forRemotes().newRequest("test").submit()).payload());
    }
}
