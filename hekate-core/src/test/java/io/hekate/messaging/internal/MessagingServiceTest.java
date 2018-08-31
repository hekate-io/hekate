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

import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.unicast.SendCallback;
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

        assertEquals(workerThreads(), channel.get().workerThreads());
        assertEquals(nioThreads(), channel.get().nioThreads());
    }

    @Test
    public void testRejoin() throws Exception {
        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> msg.reply("OK"))).join();
        TestChannel sender = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            get(sender.get().forNode(receiver.nodeId()).request("success"));

            sender.leave();

            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.get().forNode(receiver.nodeId()).request("fail"))
            );

            assertTrue(err.isCausedBy(MessagingChannelClosedException.class));

            sender.join();

            get(sender.get().forNode(receiver.nodeId()).request("success"));
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
                    from.get().forNode(to.nodeId()).send("test-" + from.nodeId());
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
                    from.get().forNode(to.nodeId()).send("test-" + from.nodeId());
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

                    from.get().forNode(to.nodeId()).send("failed", sendFailure);

                    sendFailure.awaitAndCheck(EmptyTopologyException.class);
                }
            }
        }
    }
}
