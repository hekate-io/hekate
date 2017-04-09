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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.internal.util.Waiting;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.UnknownRouteException;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.unicast.LoadBalancingException;
import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.RequestFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.TooManyRoutesException;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkService;
import io.hekate.network.internal.NetworkClientCallbackMock;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnicastMessagingTest extends MessagingServiceTestBase {
    public UnicastMessagingTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testDefaultRouting() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            MessageReceiver<String> receiver = msg -> msg.reply(msg.getChannel().getId().toString());

            c.setReceiver(receiver);
        });

        for (int i = 0; i < 50; i++) {
            Set<String> responses = new HashSet<>();

            for (TestChannel channel : channels) {
                try {
                    channel.get().request("" + i).getReply(3, TimeUnit.SECONDS);
                } catch (MessagingFutureException e) {
                    assertTrue(getStacktrace(e), e.getCause() instanceof TooManyRoutesException);
                }

                responses.add(channel.get().forOldest().request("" + i).getReply(3, TimeUnit.SECONDS));

                assertEquals(1, responses.size());
            }
        }
    }

    @Test
    public void testSendNoWait() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3);

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                from.send(to.getNodeId(), "test-" + from.getNodeId());
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test-" + from.getNodeId());
            }
        }
    }

    @Test
    public void testSendWithCallback() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3);

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.getNodeId() + "-" + to.getNodeId();

                from.sendWithSyncCallback(to.getNodeId(), msg);
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                String msg = "test-" + from.getNodeId() + "-" + to.getNodeId();

                to.awaitForMessage(msg);
            }
        }
    }

    @Test
    public void testRequestWithCallback() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

            c.setReceiver(receiver);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.getNodeId();

                assertEquals(msg + "-reply", from.requestWithSyncCallback(to.getNodeId(), msg).get());

                to.assertReceived(msg);
            }
        }
    }

    @Test
    public void testSendFuture() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3);

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.getNodeId();
                String msg2 = "test2-" + from.getNodeId();

                from.get().forNode(to.getNodeId()).send(msg1).get();
                from.get().forNode(to.getNodeId()).send(msg2).getUninterruptedly();
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test1-" + from.getNodeId());
                to.awaitForMessage("test2-" + from.getNodeId());
            }
        }
    }

    @Test
    public void testRequestFuture() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

            c.setReceiver(receiver);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.getNodeId();
                String msg2 = "test2-" + from.getNodeId();

                assertEquals(msg1 + "-reply", from.get().forNode(to.getNodeId()).request(msg1).getReply());
                assertEquals(msg2 + "-reply", from.get().forNode(to.getNodeId()).request(msg2).getReplyUninterruptedly());

                to.assertReceived(msg1);
                to.assertReceived(msg2);
            }
        }
    }

    @Test
    public void testRequestFutureUninterruptedly() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(msg -> {
            String reply = msg.get() + "-reply";

            msg.reply(reply);
        }));

        try {
            for (TestChannel from : channels) {
                for (TestChannel to : channels) {
                    String msg = "test-" + from.getNodeId();

                    Thread.currentThread().interrupt();

                    String response = from.get().forNode(to.getNodeId()).request(msg).getReplyUninterruptedly();

                    assertTrue(Thread.currentThread().isInterrupted());
                    assertEquals(msg + "-reply", response);
                }
            }
        } finally {
            if (Thread.currentThread().isInterrupted()) {
                Thread.interrupted();
            }
        }
    }

    @Test(expected = UnknownRouteException.class)
    public void testSendToUnknownWithCallback() throws Exception {
        TestChannel channel = createChannel().join();

        channel.sendWithSyncCallback(newNodeId(), "failed");
    }

    @Test
    public void testSendToUnknown() throws Exception {
        TestChannel channel = createChannel().join();

        try {
            channel.send(newNodeId(), "failed").get();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue("" + e, e.isCausedBy(UnknownRouteException.class));
        }
    }

    @Test(expected = UnknownRouteException.class)
    public void testRequestToUnknownWithCallback() throws Exception {
        TestChannel channel = createChannel().join();

        channel.requestWithSyncCallback(newNodeId(), "failed");
    }

    @Test
    public void testRequestToUnknown() throws Exception {
        TestChannel channel = createChannel().join();

        try {
            channel.request(newNodeId(), "failed").get();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue("" + e, e.isCausedBy(UnknownRouteException.class));
        }
    }

    @Test
    public void testIgnoreMessageToInvalidNode() throws Exception {
        TestChannel channel = createChannel().join();

        // Create a fake TCP client via node's TCP service.
        NetworkService net = channel.getInstance().get(NetworkService.class);

        NetworkConnector<MessagingProtocol> fakeConnector = net.connector(TEST_CHANNEL_NAME);

        repeat(5, i -> {
            NetworkClient<MessagingProtocol> fakeClient = fakeConnector.newClient();

            // Try connect to a node by using an invalid node ID.
            ClusterNodeId invalidNodeId = newNodeId();

            InetSocketAddress socketAddress = channel.getInstance().getSocketAddress();

            NetworkClientCallbackMock<MessagingProtocol> callback = new NetworkClientCallbackMock<>();

            MessagingChannelId sourceId = new MessagingChannelId();

            fakeClient.connect(socketAddress, new Connect(invalidNodeId, sourceId), callback);

            fakeClient.send(new Notification<>("fail1"));
            fakeClient.send(new Notification<>("fail2"));
            fakeClient.send(new Notification<>("fail3"));

            // Check that client was disconnected and no messages were received by the server.
            callback.awaitForDisconnects(1);
            assertTrue(channel.getReceived().isEmpty());

            fakeClient.disconnect();
        });
    }

    @Test
    public void testIdleSocketTimeoutWithSend() throws Exception {
        repeat(3, j -> {
            int idlePoolTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idlePoolTimeout)).join();

            TestChannel receiver = createChannel(c -> c.setIdleTimeout(idlePoolTimeout)).join();

            awaitForChannelsTopology(sender, receiver);

            ClientPool<String> pool = sender.getImpl().getPool(receiver.getNodeId());

            repeat(3, i -> {
                assertFalse(pool.isConnected());

                sender.send(receiver.getNodeId(), "test-" + i).get();

                busyWait("idle pool disconnect", () -> !pool.isConnected());

                assertFalse(pool.isConnected());
            });

            receiver.awaitForMessage("test-2");
            assertEquals(3, receiver.getReceived().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testIdleSocketTimeoutWithRequest() throws Exception {
        repeat(3, j -> {
            int idlePoolTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idlePoolTimeout)).join();

            TestChannel receiver = createChannel(c -> {
                c.setIdleTimeout(idlePoolTimeout);
                c.setReceiver(msg -> msg.reply("ok"));
            }).join();

            awaitForChannelsTopology(sender, receiver);

            ClientPool<String> pool = sender.getImpl().getPool(receiver.getNodeId());

            repeat(5, i -> {
                assertFalse(pool.isConnected());

                sender.request(receiver.getNodeId(), "test" + i).get();

                busyWait("idle pool disconnect", () -> !pool.isConnected());

                assertFalse(pool.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.getReceived().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testIdleTimeoutWithPendingResponse() throws Exception {
        repeat(3, j -> {
            int idlePoolTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idlePoolTimeout)).join();

            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            TestChannel receiver = createChannel(c -> {
                c.setIdleTimeout(idlePoolTimeout);
                c.setReceiver(msg -> {
                    assertTrue(msg.mustReply());

                    runAsync(() -> {
                        await(latchRef.get());

                        msg.reply("reply");

                        return null;
                    });
                });
            }).join();

            awaitForChannelsTopology(receiver, sender);

            ClientPool<String> pool = sender.getImpl().getPool(receiver.getNodeId());

            repeat(5, i -> {
                assertFalse(pool.isConnected());

                String request = "test" + i;

                RequestCallbackMock callback = new RequestCallbackMock(request);

                latchRef.set(new CountDownLatch(1));

                try {
                    // Send message.
                    sender.request(receiver.getNodeId(), request, callback);

                    // Await for timeout.
                    sleep((long)(idlePoolTimeout * 3));

                    // Pool must be still connected since there is a non-replied message.
                    assertTrue(pool.isConnected());
                } finally {
                    // Trigger reply.
                    latchRef.get().countDown();
                }

                callback.get();

                // Pool must be disconnected since there are no non-replied messages.
                busyWait("pool disconnect", () -> !pool.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.getReceived().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testSingleRequestResponse() throws Throwable {
        TestChannel sender = createChannel().join();

        AtomicReference<SendCallbackMock> replyCallbackRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());

            assertNotNull(replyCallbackRef.get());

            msg.reply("response", replyCallbackRef.get());

            assertResponded(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            replyCallbackRef.set(new SendCallbackMock());

            Reply<String> msg = sender.requestWithSyncCallback(receiver.getNodeId(), "request");

            replyCallbackRef.get().get();

            assertNotNull(msg);
            assertEquals("response", msg.get());

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testPartialResponseWithCallback() throws Throwable {
        AtomicReference<SendCallback> replyCallbackRef = new AtomicReference<>();
        AtomicReference<SendCallback> lastReplyCallbackRef = new AtomicReference<>();

        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());

            assertNotNull(replyCallbackRef.get());

            for (int i = 0; i < 3; i++) {
                msg.replyPartial("response" + i, replyCallbackRef.get());

                assertTrue(msg.mustReply());
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

            sender.request(receiver.getNodeId(), "request", (err, reply) -> {
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

    @Test
    public void testPartialResponse() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertTrue(msg.mustReply());

            for (int i = 0; i < 3; i++) {
                msg.replyPartial("response" + i);

                assertTrue(msg.mustReply());
            }

            msg.reply("final");

            assertResponded(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

            List<String> senderMessages = Collections.synchronizedList(new ArrayList<>());

            sender.request(receiver.getNodeId(), "request", (err, reply) -> {
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
    public void testAttemptToReplyOnSend() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            assertFalse(msg.mustReply());

            assertResponseUnsupported(msg);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            sender.sendWithSyncCallback(receiver.getNodeId(), "request");

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testSendAffinity() throws Exception {
        Map<Integer, List<Integer>> affinityBuf = new ConcurrentHashMap<>();

        MessageReceiver<String> affinityReceiver = msg -> {
            if (msg.get().contains(":")) {
                String[] tokens = msg.get().split(":");
                Integer key = Integer.parseInt(tokens[0]);
                Integer value = Integer.parseInt(tokens[1]);

                List<Integer> partition = affinityBuf.get(key);

                if (partition == null) {
                    partition = Collections.synchronizedList(new ArrayList<>(1000));

                    List<Integer> existing = affinityBuf.putIfAbsent(key, partition);

                    if (existing != null) {
                        partition = existing;
                    }
                }

                partition.add(value);
            }
        };

        TestChannel receiver = createChannel(c -> c.setReceiver(affinityReceiver)).join();

        TestChannel sender = createChannel().join();

        sender.awaitForTopology(Arrays.asList(sender, receiver));

        int partitionSize = 50;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                sender.get().forNode(receiver.getNodeId()).affinitySend(j, j + ":" + i);
            }
        }

        for (int i = 0; i < partitionSize; i++) {
            receiver.awaitForMessage(i + ":" + (partitionSize - 1));
        }

        for (List<Integer> partition : affinityBuf.values()) {
            for (int i = 0; i < partitionSize; i++) {
                assertEquals(i, partition.get(i).intValue());
            }
        }
    }

    @Test
    public void testRequestAffinity() throws Exception {
        Map<Integer, List<Integer>> affinityBuf = new ConcurrentHashMap<>();

        MessageReceiver<String> affinityReceiver = msg -> {
            if (msg.get().contains(":")) {
                String[] tokens = msg.get().split(":");
                Integer key = Integer.parseInt(tokens[0]);
                Integer value = Integer.parseInt(tokens[1]);

                List<Integer> partition = affinityBuf.get(key);

                if (partition == null) {
                    partition = Collections.synchronizedList(new ArrayList<>(1000));

                    List<Integer> existing = affinityBuf.putIfAbsent(key, partition);

                    if (existing != null) {
                        partition = existing;
                    }
                }

                partition.add(value);
            }

            msg.reply("ok");
        };

        TestChannel receiver = createChannel(c -> c.setReceiver(affinityReceiver)).join();

        TestChannel sender = createChannel().join();

        sender.awaitForTopology(Arrays.asList(sender, receiver));

        int partitionSize = 50;

        sayTime("Request-response", () -> {
            for (int i = 0; i < partitionSize; i++) {
                List<CompletableFuture<?>> futures = new ArrayList<>(partitionSize * partitionSize);

                for (int j = 0; j < partitionSize; j++) {
                    futures.add(sender.get().forNode(receiver.getNodeId()).affinityRequest(j, j + ":" + i));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            }
        });

        for (List<Integer> partition : affinityBuf.values()) {
            for (int i = 0; i < partitionSize; i++) {
                assertEquals(i, partition.get(i).intValue());
            }
        }
    }

    @Test
    public void testSingleRequestThreadAffinity() throws Exception {
        TestChannel sender = createChannel().join();

        AtomicReference<SendCallbackMock> callbackRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            Thread thread = Thread.currentThread();

            SendCallbackMock callback = new SendCallbackMock() {
                @Override
                protected void onSendSuccess() {
                    assertSame(Thread.currentThread(), thread);
                }
            };

            callbackRef.set(callback);

            msg.reply("response", callback);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            sender.requestWithSyncCallback(receiver.getNodeId(), "request");

            assertNotNull(callbackRef.get());

            callbackRef.get().get();

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testServerResponseFailureOnServer() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            throw TEST_ERROR;
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            String msg = "request" + i;

            try {
                sender.requestWithSyncCallback(receiver.getNodeId(), msg);

                fail("Error was expected.");
            } catch (ClosedChannelException e) {
                // No-op.
            }

            receiver.awaitForMessage(msg);
        });
    }

    @Test
    public void testServerResponseFailureOnClient() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        ClientPool<String> pool = sender.getImpl().getPool(receiver.getNodeId());

        List<NetworkFuture<MessagingProtocol>> closeFuture = pool.close();

        for (NetworkFuture<MessagingProtocol> future : closeFuture) {
            future.get();
        }

        repeat(5, i -> {
            try {
                sender.requestWithSyncCallback(receiver.getNodeId(), "request" + i);

                fail("Error was expected.");
            } catch (ClosedChannelException e) {
                // No-op.
            }
        });
    }

    @Test
    public void testClientRequestFailure() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        ClientPool<String> pool = sender.getImpl().getPool(receiver.getNodeId());

        List<NetworkFuture<MessagingProtocol>> closeFuture = pool.close();

        for (NetworkFuture<MessagingProtocol> future : closeFuture) {
            future.get();
        }

        repeat(5, i -> {
            try {
                sender.sendWithSyncCallback(receiver.getNodeId(), "request" + i);

                fail("Error was expected.");
            } catch (ClosedChannelException e) {
                // No-op.
            }
        });
    }

    @Test
    public void testChannelCloseWhileSending() throws Exception {
        repeat(3, i -> {
            AtomicInteger received = new AtomicInteger();
            CountDownLatch receivedLatch = new CountDownLatch(1);

            TestChannel sender = createChannel().join();

            TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
                if (received.incrementAndGet() == 100) {
                    receivedLatch.countDown();
                }

                msg.reply("response");
            })).join();

            awaitForChannelsTopology(sender, receiver);

            AtomicBoolean stopped = new AtomicBoolean();

            Future<Void> future = runAsync(() -> {
                while (!stopped.get()) {
                    sender.requestWithSyncCallback(receiver.getNodeId(), "request");
                }

                // Last attempt to make sure that we really tried to send a message after channel was closed.
                sender.requestWithSyncCallback(receiver.getNodeId(), "request");

                return null;
            });

            await(receivedLatch);

            sender.getImpl().close().await();

            stopped.set(true);

            try {
                get(future);

                fail("Error was expected.");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof LoadBalancingException) {
                    assertTrue(cause.getMessage(), cause.getMessage().contains("Node is not within the channel topology"));
                } else if (cause instanceof MessagingChannelClosedException) {
                    assertEquals("Channel closed [channel=test_channel]", cause.getMessage());
                } else {
                    assertTrue(getStacktrace(cause), cause instanceof ClosedChannelException);
                }
            }

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testChannelCloseWhileReceiving() throws Exception {
        repeat(3, i -> {
            AtomicInteger received = new AtomicInteger();

            CountDownLatch receivedLatch = new CountDownLatch(1);

            TestChannel sender = createChannel().join();
            TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
                if (received.incrementAndGet() == 100) {
                    receivedLatch.countDown();
                }

                msg.reply("response");
            })).join();

            awaitForChannelsTopology(sender, receiver);

            AtomicBoolean stopped = new AtomicBoolean();

            Future<Void> future = runAsync(() -> {
                while (!stopped.get()) {
                    sender.requestWithSyncCallback(receiver.getNodeId(), "request");
                }

                // Last attempt to make sure that we really tried to send a message after channel was closed.
                sender.requestWithSyncCallback(receiver.getNodeId(), "request");

                return null;
            });

            await(receivedLatch);

            receiver.getImpl().close().await();

            stopped.set(true);

            try {
                future.get();

                fail("Error was expected.");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), e.getCause() instanceof IOException);
            }

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testChannelCloseWhileRoutingOnSend() throws Exception {
        repeat(3, i -> {
            TestChannel sender = createChannel().join();
            TestChannel receiver = createChannel().join();

            awaitForChannelsTopology(sender, receiver);

            CountDownLatch routeLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            Future<SendFuture> future = runAsync(() -> sender.withLoadBalancer((msg, topology) -> {
                routeLatch.countDown();

                await(closeLatch);

                return receiver.getNodeId();
            }).send("test"));

            await(routeLatch);

            Waiting close = sender.getImpl().close();

            closeLatch.countDown();

            close.await();

            try {
                get(get(future));

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                Throwable cause = e.getCause();

                assertTrue(getStacktrace(cause), cause instanceof MessagingChannelClosedException);
            }

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testChannelCloseWhileRoutingOnRequest() throws Exception {
        repeat(3, i -> {
            TestChannel sender = createChannel().join();
            TestChannel receiver = createChannel().join();

            awaitForChannelsTopology(sender, receiver);

            CountDownLatch routeLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            Future<RequestFuture<String>> future = runAsync(() -> sender.withLoadBalancer((msg, topology) -> {
                routeLatch.countDown();

                await(closeLatch);

                return receiver.getNodeId();
            }).request("test"));

            await(routeLatch);

            Waiting close = sender.getImpl().close();

            closeLatch.countDown();

            close.await();

            try {
                get(get(future));

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                Throwable cause = e.getCause();

                assertTrue(getStacktrace(cause), cause instanceof MessagingChannelClosedException);
            }

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testTopologyChangeWhileSending() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            say("Topology change on join.");

            CountDownLatch beforeJoinLatch = new CountDownLatch(1);
            CountDownLatch joinLatch = new CountDownLatch(1);
            AtomicInteger joinInvocations = new AtomicInteger();
            SendCallbackMock joinCallback = new SendCallbackMock();

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeJoinLatch.countDown();

                    joinInvocations.incrementAndGet();

                    await(joinLatch);

                    return topology.getYoungest().getId();
                }).send("join-request-" + i, joinCallback);

                return null;
            });

            await(beforeJoinLatch);

            TestChannel temporary = createChannel().join();

            awaitForChannelsTopology(sender, receiver, temporary);

            joinLatch.countDown();

            joinCallback.get();

            receiver.awaitForMessage("join-request-" + i);

            assertEquals(1, joinInvocations.get());

            say("Topology change on leave.");

            CountDownLatch beforeLeaveLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);
            AtomicInteger leaveInvocations = new AtomicInteger();
            SendCallbackMock leaveCallback = new SendCallbackMock();

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeLeaveLatch.countDown();

                    leaveInvocations.incrementAndGet();

                    await(leaveLatch);

                    return topology.getYoungest().getId();
                }).send("leave-request-" + i, leaveCallback);

                return null;
            });

            await(beforeLeaveLatch);

            temporary.leave();

            awaitForChannelsTopology(sender, receiver);

            leaveLatch.countDown();

            expect(UnknownRouteException.class, leaveCallback::get);

            assertEquals(1, leaveInvocations.get());
        });
    }

    @Test
    public void testTopologyChangeWhileRequesting() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = "response-" + msg.get();

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            say("Topology change on join.");

            String joinMsg = "join-request-" + i;

            CountDownLatch beforeJoinLatch = new CountDownLatch(1);
            CountDownLatch joinLatch = new CountDownLatch(1);
            AtomicInteger joinInvocations = new AtomicInteger();
            RequestCallbackMock joinCallback = new RequestCallbackMock(joinMsg);

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeJoinLatch.countDown();

                    joinInvocations.incrementAndGet();

                    await(joinLatch);

                    return topology.getYoungest().getId();
                }).request(joinMsg, joinCallback);

                return null;
            });

            await(beforeJoinLatch);

            TestChannel temporary = createChannel(c -> c.setReceiver(msg -> {
                String response = "response-" + msg.get();

                msg.reply(response);
            })).join();

            awaitForChannelsTopology(sender, receiver, temporary);

            joinLatch.countDown();

            assertEquals("response-" + joinMsg, joinCallback.get().get());

            receiver.awaitForMessage(joinMsg);

            assertEquals(1, joinInvocations.get());

            say("Topology change on leave.");

            String leaveMsg = "leave-request-" + i;

            CountDownLatch beforeLeaveLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);
            AtomicInteger leaveInvocations = new AtomicInteger();
            RequestCallbackMock leaveCallback = new RequestCallbackMock(leaveMsg);

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeLeaveLatch.countDown();

                    leaveInvocations.incrementAndGet();

                    await(leaveLatch);

                    return topology.getYoungest().getId();
                }).request(leaveMsg, leaveCallback);

                return null;
            });

            await(beforeLeaveLatch);

            temporary.leave();

            awaitForChannelsTopology(sender, receiver);

            leaveLatch.countDown();

            expect(UnknownRouteException.class, leaveCallback::get);

            assertEquals(1, leaveInvocations.get());
        });
    }

    @Test
    public void testRequestOnClusterEvent() throws Throwable {
        TestChannel channel = createChannel(c -> c.withReceiver(msg -> msg.reply("ok")));

        AtomicReference<Throwable> errRef = new AtomicReference<>();
        RequestCallbackMock toSelf = new RequestCallbackMock("to-self");
        RequestCallbackMock toRemote = new RequestCallbackMock("to-remote");

        channel.getInstance().get(ClusterService.class).addListener(event -> {
            try {
                MessagingChannel<String> send = event.getHekate().get(MessagingService.class).channel(TEST_CHANNEL_NAME);

                if (event.getType() == ClusterEventType.JOIN) {
                    get(send.request("to-self"));
                    send.request("to-self", toSelf);
                } else if (event.getType() == ClusterEventType.CHANGE) {
                    get(send.forRemotes().request("to-remote"));
                    send.forRemotes().request("to-remote", toRemote);
                }
            } catch (Throwable err) {
                errRef.compareAndSet(null, err);
            }
        });

        channel.join();

        assertEquals("ok", toSelf.get().get());

        if (errRef.get() != null) {
            throw errRef.get();
        }

        createChannel(c -> c.withReceiver(msg -> msg.reply("ok"))).join();

        assertEquals("ok", toRemote.get().get());

        if (errRef.get() != null) {
            throw errRef.get();
        }
    }

    @Test
    public void testLoadBalanceFailureOnSend() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            SendFuture future = sender.withLoadBalancer((msg, topology) -> {
                throw new TestHekateException(TEST_ERROR_MESSAGE);
            }).send("failed" + i);

            try {
                future.get();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(TestHekateException.class));
                assertEquals(TEST_ERROR_MESSAGE, e.getCause().getMessage());
            }
        });

        sender.send(receiver.getNodeId(), "success").get();
    }

    @Test
    public void testLoadBalanceFailureOnRequest() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            RequestFuture<String> future = sender.withLoadBalancer((msg, topology) -> {
                throw new TestHekateException(TEST_ERROR_MESSAGE);
            }).request("failed" + i);

            try {
                future.getReply();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(TestHekateException.class));
                assertEquals(TEST_ERROR_MESSAGE, e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").getReply());
    }

    @Test
    public void testLoadBalanceReturnsNullOnSend() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            SendFuture future = sender.withLoadBalancer((msg, topology) -> null).send("failed" + i);

            try {
                future.get();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(LoadBalancingException.class));
                assertEquals("Load balancer failed to select a target node.", e.getCause().getMessage());
            }
        });

        sender.send(receiver.getNodeId(), "success").get();
    }

    @Test
    public void testLoadBalanceReturnsNullOnRequest() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            RequestFuture<String> future = sender.withLoadBalancer((msg, topology) -> null).request("failed" + i);

            try {
                future.getReply();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(LoadBalancingException.class));
                assertEquals("Load balancer failed to select a target node.", e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").getReply());
    }

    @Test
    public void testRouteToNonExistingNodeOnSend() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        ClusterNodeId invalidNodeId = newNodeId();

        repeat(3, i -> {
            SendFuture future = sender.withLoadBalancer((msg, topology) -> invalidNodeId).send("failed" + i);

            try {
                future.get();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(UnknownRouteException.class));
                assertEquals("Node is not within the channel topology [id=" + invalidNodeId + ']', e.getCause().getMessage());
            }
        });

        sender.send(receiver.getNodeId(), "success").get();
    }

    @Test
    public void testRouteToNonExistingNodeOnRequest() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        ClusterNodeId invalidNodeId = newNodeId();

        repeat(3, i -> {
            RequestFuture<String> future = sender.withLoadBalancer((msg, topology) -> invalidNodeId).request("failed" + i);

            try {
                future.getReply();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(UnknownRouteException.class));
                assertEquals("Node is not within the channel topology [id=" + invalidNodeId + ']', e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").getReply());
    }

    @Test
    public void testTooManyRoutesOnRequest() throws Throwable {
        MessageReceiver<String> receiver = msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        };

        TestChannel channel1 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel2 = createChannel(c -> c.setReceiver(receiver)).join();

        awaitForChannelsTopology(channel1, channel2);

        repeat(3, i -> {
            RequestFuture<String> future = channel1.get().request("failed" + i);

            try {
                future.getReply();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(TooManyRoutesException.class));
                assertEquals("Too many receivers "
                        + "[channel=" + channel1.get().getName() + ", topology=" + channel1.get().getCluster().getTopology() + ']',
                    e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", channel1.request(channel2.getNodeId(), "success").getReply());
    }

    @Test
    public void testMessagingEndpoint() throws Exception {
        TestChannel sender = createChannel().join();

        AtomicReference<AssertionError> errorRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            try {
                MessagingEndpoint<String> endpoint = msg.getEndpoint();

                assertEquals(sender.getId(), endpoint.getRemoteId());

                assertTrue(endpoint.toString(), endpoint.toString().startsWith(MessagingEndpoint.class.getSimpleName()));
            } catch (AssertionError e) {
                errorRef.compareAndSet(null, e);
            }

            if (msg.mustReply()) {
                String response = msg.get() + "-reply";

                msg.reply(response);
            }
        })).join();

        awaitForChannelsTopology(sender, receiver);

        sender.get().forNode(receiver.getNodeId()).request("request").get();

        if (errorRef.get() != null) {
            throw errorRef.get();
        }
    }

    @Test
    public void testSendWithNoReceivers() throws Exception {
        TestChannel channel = createChannel(c ->
            c.withClusterFilter(n -> !n.isLocal())
        ).join();

        try {
            get(channel.send(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof LoadBalancingException);
            assertEquals("No suitable receivers [channel=test_channel]", e.getCause().getMessage());
        }

        try {
            get(channel.request(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof LoadBalancingException);
            assertEquals("No suitable receivers [channel=test_channel]", e.getCause().getMessage());
        }

        try {
            channel.sendWithSyncCallback(channel.getNodeId(), "test");
        } catch (LoadBalancingException e) {
            assertEquals("No suitable receivers [channel=test_channel]", e.getMessage());
        }

        try {
            channel.requestWithSyncCallback(channel.getNodeId(), "test");
        } catch (LoadBalancingException e) {
            assertEquals("No suitable receivers [channel=test_channel]", e.getMessage());
        }
    }

    @Test
    public void testClosedChannel() throws Exception {
        TestChannel channel = createChannel();

        channel.join();

        get(channel.send(channel.getNodeId(), "test"));

        channel.leave();

        try {
            get(channel.send(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingChannelClosedException);
            assertEquals("Channel closed [channel=test_channel]", e.getCause().getMessage());
        }

        try {
            get(channel.request(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingChannelClosedException);
            assertEquals("Channel closed [channel=test_channel]", e.getCause().getMessage());
        }

        try {
            channel.sendWithSyncCallback(channel.getNodeId(), "test");
        } catch (MessagingChannelClosedException e) {
            assertEquals("Channel closed [channel=test_channel]", e.getMessage());
        }

        try {
            channel.requestWithSyncCallback(channel.getNodeId(), "test");
        } catch (MessagingChannelClosedException e) {
            assertEquals("Channel closed [channel=test_channel]", e.getMessage());
        }
    }

    @Test
    public void testServerReplyAfterStop() throws Exception {
        TestChannel sender = createChannel().join();

        CompletableFuture<Message<String>> requestFuture = new CompletableFuture<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(requestFuture::complete)).join();

        awaitForChannelsTopology(sender, receiver);

        sender.request(receiver.getNodeId(), "test");

        Message<String> request = get(requestFuture);

        receiver.leave();

        Throwable err = replyAndGetError(request);

        assertNotNull(err);
        assertTrue(err.toString(), err instanceof ClosedChannelException);
    }

    private Throwable replyAndGetError(Message<String> reply) throws Exception {
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        runAsync(() -> {
            reply.reply("reply", errFuture::complete);

            return null;
        });

        return get(errFuture);
    }

    private void assertResponded(Message<String> msg) {
        assertFalse(msg.mustReply());

        try {
            msg.reply("invalid");

            fail("Failure was expected.");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Message already responded"));
        }

        try {
            msg.reply("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Message already responded"));
        }

        try {
            msg.replyPartial("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Message already responded"));
        }
    }

    private void assertResponseUnsupported(Message<String> msg) {
        try {
            msg.reply("invalid");

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Responses is not expected.", e.getMessage());
        }

        try {
            msg.reply("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Responses is not expected.", e.getMessage());
        }

        try {
            msg.replyPartial("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Responses is not expected.", e.getMessage());
        }
    }
}
