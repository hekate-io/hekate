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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.CodecException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageInterceptor;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.network.NetworkFuture;
import io.hekate.test.HekateTestError;
import io.hekate.test.NonDeserializable;
import io.hekate.test.NonSerializable;
import io.hekate.util.async.Waiting;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingChannelRequestTest extends MessagingServiceTestBase {
    public MessagingChannelRequestTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testCallback() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.setReceiver(msg ->
                msg.reply(msg.get() + "-reply")
            )
        );

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.nodeId();

                assertEquals(msg + "-reply", from.requestWithSyncCallback(to.nodeId(), msg).get());

                to.assertReceived(msg);
            }
        }
    }

    @Test
    public void testFuture() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

            c.setReceiver(receiver);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.nodeId();
                String msg2 = "test2-" + from.nodeId();

                assertEquals(msg1 + "-reply", from.get().forNode(to.nodeId()).request(msg1).response());
                assertEquals(msg1 + "-reply", from.get().forNode(to.nodeId()).request(msg1).response(3, TimeUnit.SECONDS));
                assertEquals(msg2 + "-reply", from.get().forNode(to.nodeId()).request(msg2).responseUninterruptedly());

                assertEquals(msg1 + "-reply", from.get().forNode(to.nodeId()).request(msg1).response(String.class));
                assertEquals(msg1 + "-reply", from.get().forNode(to.nodeId()).request(msg1).response(String.class, 3, TimeUnit.SECONDS));
                assertEquals(msg2 + "-reply", from.get().forNode(to.nodeId()).request(msg2).responseUninterruptedly(String.class));

                to.assertReceived(msg1);
                to.assertReceived(msg2);
            }
        }
    }

    @Test
    public void testReplyCallback() throws Throwable {
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

            Response<String> msg = sender.requestWithSyncCallback(receiver.nodeId(), "request");

            replyCallbackRef.get().get();

            assertNotNull(msg);
            assertEquals("response", msg.get());

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testFutureUninterruptedly() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(msg -> {
            String reply = msg.get() + "-reply";

            msg.reply(reply);
        }));

        try {
            for (TestChannel from : channels) {
                for (TestChannel to : channels) {
                    String msg = "test-" + from.nodeId();

                    Thread.currentThread().interrupt();

                    String response = from.get().forNode(to.nodeId()).request(msg).responseUninterruptedly();

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

    @Test(expected = EmptyTopologyException.class)
    public void testUnknownNodeCallback() throws Exception {
        TestChannel channel = createChannel().join();

        channel.requestWithSyncCallback(newNodeId(), "failed");
    }

    @Test
    public void testUnknownNodeFuture() throws Exception {
        TestChannel channel = createChannel().join();

        try {
            channel.get().forNode(newNodeId()).request("failed").get();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue("" + e, e.isCausedBy(EmptyTopologyException.class));
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleSocketTimeout(idleTimeout)).join();

            TestChannel receiver = createChannel(c -> {
                c.setIdleSocketTimeout(idleTimeout);
                c.setReceiver(msg -> msg.reply("ok"));
            }).join();

            awaitForChannelsTopology(sender, receiver);

            MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                sender.get().forNode(receiver.nodeId()).request("test" + i).get();

                busyWait("disconnect idle", () -> !client.isConnected());

                assertFalse(client.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.received().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testNoFailuresWithSmallIdleTimeout() throws Exception {
        TestChannel sender = createChannel(c -> c.setIdleSocketTimeout(25)).join();

        TestChannel receiver = createChannel(c -> {
            c.setIdleSocketTimeout(50);
            c.setReceiver(msg -> msg.reply("ok"));
        }).join();

        awaitForChannelsTopology(sender, receiver);

        runParallel(4, 1000, s ->
            sender.get().forNode(receiver.nodeId()).request("test").get()
        );

        sender.leave();
        receiver.leave();
    }

    @Test
    public void testIdleTimeoutWithPendingResponse() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleSocketTimeout(idleTimeout)).join();

            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            TestChannel receiver = createChannel(c -> {
                c.setIdleSocketTimeout(idleTimeout);
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

            MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                String request = "test" + i;

                ResponseCallbackMock callback = new ResponseCallbackMock(request);

                latchRef.set(new CountDownLatch(1));

                try {
                    // Send message.
                    sender.get().forNode(receiver.nodeId()).request(request, callback);

                    // Await for timeout.
                    sleep((long)(idleTimeout * 3));

                    // Client must be still connected since there is a non-replied message.
                    assertTrue(client.isConnected());
                } finally {
                    // Trigger reply.
                    latchRef.get().countDown();
                }

                callback.get();

                // Pool must be disconnected since there are no non-replied messages.
                busyWait("pool disconnect", () -> !client.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.received().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testResponseFailure() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            throw TEST_ERROR;
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            String msg = "request" + i;

            try {
                sender.requestWithSyncCallback(receiver.nodeId(), msg);

                fail("Error was expected.");
            } catch (MessagingRemoteException e) {
                assertTrue(e.remoteStackTrace().contains(HekateTestError.MESSAGE));
            }

            receiver.awaitForMessage(msg);
        });
    }

    @Test
    public void testNetworkDisconnectWhileSending() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

        List<NetworkFuture<MessagingProtocol>> closeFuture = client.close();

        for (NetworkFuture<MessagingProtocol> future : closeFuture) {
            future.get();
        }

        repeat(5, i -> {
            try {
                sender.requestWithSyncCallback(receiver.nodeId(), "request" + i);

                fail("Error was expected.");
            } catch (MessagingException e) {
                assertTrue(e.toString(), e.isCausedBy(ClosedChannelException.class));
            }
        });
    }

    @Test
    public void testNetworkDisconnectWhileReceiving() throws Throwable {
        TestChannel sender = createChannel().join();

        AtomicReference<MessagingClient<String>> clientRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c ->
            c.withReceiver(msg ->
                clientRef.get().close()
            )
        ).join();

        awaitForChannelsTopology(sender, receiver);

        MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

        clientRef.set(client);

        repeat(5, i -> {
            try {
                sender.requestWithSyncCallback(receiver.nodeId(), "request" + i);

                fail("Error was expected.");
            } catch (MessagingException e) {
                // No-op.
            }
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

        sender.get().forNode(receiver.nodeId()).request("test");

        Message<String> msg = messageExchanger.exchange(null, 3, TimeUnit.SECONDS);

        receiver.leave();

        Exchanger<Throwable> errExchanger = new Exchanger<>();

        msg.reply("fail", err -> {
            try {
                errExchanger.exchange(err);
            } catch (InterruptedException e) {
                throw new AssertionError("Thread was unexpectedly interrupted.", e);
            }
        });

        Throwable err = errExchanger.exchange(null, 3, TimeUnit.SECONDS);

        assertTrue(getStacktrace(err), err instanceof MessagingException);
        assertTrue(getStacktrace(err), ErrorUtils.isCausedBy(ClosedChannelException.class, err));
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
                    sender.requestWithSyncCallback(receiver.nodeId(), "request");
                }

                // Last attempt to make sure that we really tried to send a message after channel was closed.
                sender.requestWithSyncCallback(receiver.nodeId(), "request");

                return null;
            });

            await(receivedLatch);

            sender.impl().close().await();

            stopped.set(true);

            try {
                get(future);

                fail("Error was expected.");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof LoadBalancerException) {
                    assertTrue(cause.getMessage(), cause.getMessage().contains("Node is not within the channel topology"));
                } else if (cause instanceof MessagingChannelClosedException) {
                    assertEquals("Channel closed [channel=test-channel]", cause.getMessage());
                } else {
                    assertTrue(getStacktrace(cause), cause instanceof MessagingException);
                    assertTrue(getStacktrace(cause), ErrorUtils.isCausedBy(ClosedChannelException.class, cause));
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
                    sender.requestWithSyncCallback(receiver.nodeId(), "request");
                }

                // Last attempt to make sure that we really tried to send a message after channel was closed.
                sender.requestWithSyncCallback(receiver.nodeId(), "request");

                return null;
            });

            await(receivedLatch);

            receiver.impl().close().await();

            stopped.set(true);

            try {
                future.get();

                fail("Error was expected.");
            } catch (ExecutionException e) {
                assertTrue(ErrorUtils.stackTrace(e), ErrorUtils.isCausedBy(MessagingException.class, e));
                assertTrue(ErrorUtils.stackTrace(e), ErrorUtils.isCausedBy(IOException.class, e));
            }

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testChannelCloseWhileRouting() throws Exception {
        repeat(3, i -> {
            TestChannel sender = createChannel().join();
            TestChannel receiver = createChannel().join();

            awaitForChannelsTopology(sender, receiver);

            CountDownLatch routeLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            Future<ResponseFuture<String>> future = runAsync(() -> sender.withLoadBalancer((msg, topology) -> {
                routeLatch.countDown();

                await(closeLatch);

                return receiver.nodeId();
            }).request("test"));

            await(routeLatch);

            Waiting close = sender.impl().close();

            closeLatch.countDown();

            close.await();

            try {
                ResponseFuture<String> request = get(future);

                get(request);

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
    public void testTopologyChange() throws Throwable {
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
            ResponseCallbackMock joinCallback = new ResponseCallbackMock(joinMsg);

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeJoinLatch.countDown();

                    joinInvocations.incrementAndGet();

                    await(joinLatch);

                    return topology.youngest().id();
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
            ResponseCallbackMock leaveCallback = new ResponseCallbackMock(leaveMsg);

            runAsync(() -> {
                sender.withLoadBalancer((msg, topology) -> {
                    beforeLeaveLatch.countDown();

                    leaveInvocations.incrementAndGet();

                    await(leaveLatch);

                    return topology.youngest().id();
                }).request(leaveMsg, leaveCallback);

                return null;
            });

            await(beforeLeaveLatch);

            temporary.leave();

            awaitForChannelsTopology(sender, receiver);

            leaveLatch.countDown();

            leaveCallback.get();

            assertEquals(2, leaveInvocations.get());
        });
    }

    @Test
    public void testRequestOnClusterEvent() throws Throwable {
        TestChannel channel = createChannel(c -> c.withReceiver(msg -> msg.reply("ok")));

        AtomicReference<Throwable> errRef = new AtomicReference<>();
        ResponseCallbackMock toSelfCallback = new ResponseCallbackMock("to-self");
        ResponseCallbackMock toRemoteCallback = new ResponseCallbackMock("to-remote");

        channel.node().cluster().addListener(event -> {
            try {
                MessagingChannel<String> send = channel.node().messaging().channel(TEST_CHANNEL_NAME, String.class);

                if (event.type() == ClusterEventType.JOIN) {
                    get(send.request("to-self"));

                    send.request("to-self", toSelfCallback);
                } else if (event.type() == ClusterEventType.CHANGE) {
                    get(send.forRemotes().request("to-remote"));

                    send.forRemotes().request("to-remote", toRemoteCallback);
                }
            } catch (Throwable err) {
                errRef.compareAndSet(null, err);
            }
        });

        channel.join();

        assertEquals("ok", toSelfCallback.get().get());

        if (errRef.get() != null) {
            throw errRef.get();
        }

        createChannel(c -> c.withReceiver(msg -> msg.reply("ok"))).join();

        assertEquals("ok", toRemoteCallback.get().get());

        if (errRef.get() != null) {
            throw errRef.get();
        }
    }

    @Test
    public void testLoadBalanceFailure() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            ResponseFuture<String> future = sender.withLoadBalancer((msg, topology) -> {
                throw new LoadBalancerException(HekateTestError.MESSAGE);
            }).request("failed" + i);

            try {
                future.response();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(LoadBalancerException.class));
                assertEquals(HekateTestError.MESSAGE, e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.get().forNode(receiver.nodeId()).request("success").response());
    }

    @Test
    public void testLoadBalanceReturnsNull() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            ResponseFuture<String> future = sender.withLoadBalancer((msg, topology) -> null).request("failed" + i);

            try {
                future.response();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(LoadBalancerException.class));
                assertEquals("Load balancer failed to select a target node.", e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.get().forNode(receiver.nodeId()).request("success").response());
    }

    @Test
    public void testRouteToNonExistingNode() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        ClusterNodeId invalidNodeId = newNodeId();

        repeat(3, i -> {
            ResponseFuture<String> future = sender.withLoadBalancer((msg, topology) -> invalidNodeId).request("failed" + i);

            try {
                future.response();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(UnknownRouteException.class));
                assertEquals("Node is not within the channel topology [id=" + invalidNodeId + ']', e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.get().forNode(receiver.nodeId()).request("success").response());
    }

    @Test
    public void testNonAffinityRouting() throws Throwable {
        Map<ClusterNode, List<String>> received = new ConcurrentHashMap<>();

        MessageReceiver<String> receiver = msg -> {
            ClusterNode localNode = msg.channel().cluster().topology().localNode();

            received.computeIfAbsent(localNode, n -> synchronizedList(new ArrayList<>())).add(msg.get());

            msg.reply(msg.get() + "-reply");
        };

        TestChannel channel1 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel2 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel3 = createChannel(c -> c.setReceiver(receiver)).join();

        awaitForChannelsTopology(channel1, channel2, channel3);

        for (int i = 0; i < 100; i++) {
            get(channel1.get().request("test" + i));
        }

        List<String> received1 = received.get(channel1.node().localNode());
        List<String> received2 = received.get(channel2.node().localNode());
        List<String> received3 = received.get(channel3.node().localNode());

        assertNotNull(received1);
        assertNotNull(received2);
        assertNotNull(received3);

        assertFalse(received1.isEmpty());
        assertFalse(received2.isEmpty());
        assertFalse(received3.isEmpty());

        assertEquals(100, received1.size() + received2.size() + received3.size());
    }

    @Test
    public void testAffinityRouting() throws Throwable {
        Map<ClusterNode, List<String>> received = new ConcurrentHashMap<>();

        MessageReceiver<String> receiver = msg -> {
            ClusterNode localNode = msg.channel().cluster().topology().localNode();

            received.computeIfAbsent(localNode, n -> synchronizedList(new ArrayList<>())).add(msg.get());

            msg.reply(msg.get() + "-reply");
        };

        TestChannel channel1 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel2 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel3 = createChannel(c -> c.setReceiver(receiver)).join();

        awaitForChannelsTopology(channel1, channel2, channel3);

        Set<ClusterNode> uniqueNodes = new HashSet<>();

        repeat(25, j -> {
            received.clear();

            for (int i = 0; i < 50; i++) {
                get(channel1.get().withAffinity(j).request("test" + i));
            }

            List<ClusterNode> singleNode = received.entrySet().stream()
                .filter(e -> e.getValue().size() == 50)
                .map(Map.Entry::getKey)
                .collect(toList());

            assertEquals(1, singleNode.size());
            uniqueNodes.addAll(singleNode);
        });

        assertEquals(3, uniqueNodes.size());
    }

    @Test
    public void testNoReceiver() throws Exception {
        TestChannel channel = createChannel(c ->
            c.withClusterFilter(n -> !n.isLocal())
        ).join();

        try {
            get(channel.get().forNode(channel.nodeId()).request("test"));

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof LoadBalancerException);
            assertEquals("No suitable receivers [channel=test-channel]", e.getCause().getMessage());
        }

        try {
            channel.requestWithSyncCallback(channel.nodeId(), "test");

            fail("Error was expected.");
        } catch (LoadBalancerException e) {
            assertEquals("No suitable receivers [channel=test-channel]", e.getMessage());
        }
    }

    @Test
    public void testClosedChannel() throws Exception {
        TestChannel channel = createChannel();

        channel.join();

        get(channel.get().forNode(channel.nodeId()).send("test"));

        channel.leave();

        try {
            get(channel.get().forNode(channel.nodeId()).request("test"));

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingChannelClosedException);
            assertEquals("Channel closed [channel=test-channel]", e.getCause().getMessage());
        }

        try {
            channel.requestWithSyncCallback(channel.nodeId(), "test");

            fail("Error was expected.");
        } catch (MessagingChannelClosedException e) {
            assertEquals("Channel closed [channel=test-channel]", e.getMessage());
        }
    }

    @Test
    public void testServerReplyAfterStop() throws Exception {
        TestChannel sender = createChannel().join();

        CompletableFuture<Message<String>> requestFuture = new CompletableFuture<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(requestFuture::complete)).join();

        awaitForChannelsTopology(sender, receiver);

        sender.get().forNode(receiver.nodeId()).request("test");

        Message<String> request = get(requestFuture);

        receiver.leave();

        MessagingException err = (MessagingException)replyAndGetError(request);

        assertNotNull(err);
        assertTrue(err.toString(), err.getCause() instanceof ClosedChannelException);
    }

    @Test
    public void testInterceptor() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.setReceiver(msg -> msg.reply(msg.get() + "-reply"));

            c.setInterceptor(new MessageInterceptor<String>() {
                @Override
                public String interceptOutbound(String msg, OutboundContext ctx) {
                    return msg + "-###";
                }

                @Override
                public String interceptInbound(String msg, InboundContext ctx) {
                    return msg + "-@@@";
                }

                @Override
                public String interceptReply(String msg, ReplyContext ctx) {
                    return msg + "-$$$";
                }
            });
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.nodeId();
                String msg2 = "test2-" + from.nodeId();

                assertEquals(msg1 + "-###-@@@-reply-$$$-@@@", from.get().forNode(to.nodeId()).request(msg1).response());
                assertEquals(msg2 + "-###-@@@-reply-$$$-@@@", from.get().forNode(to.nodeId()).request(msg2).responseUninterruptedly());

                to.assertReceived(msg1 + "-###-@@@");
                to.assertReceived(msg2 + "-###-@@@");
            }
        }
    }

    @Test
    public void testNonSerializableRequest() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver(msg ->
            msg.reply("OK")
        );

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test").forRemotes().request(new NonSerializable()))
            );

            assertSame(err.toString(), MessagingException.class, err.getCause().getClass());
            assertTrue(err.isCausedBy(CodecException.class));
            assertTrue(err.isCausedBy(NotSerializableException.class));
        });
    }

    @Test
    public void testNonDeSerializableRequest() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver(msg ->
            msg.reply("OK")
        );

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test").forRemotes().request(new NonDeserializable()))
            );

            assertSame(err.toString(), MessagingRemoteException.class, err.getCause().getClass());
            assertTrue(ErrorUtils.stackTrace(err).contains(HekateTestError.MESSAGE));
        });
    }

    @Test
    public void testNonSerializableResponse() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver(msg ->
            msg.reply(new Socket())
        );

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test").forRemotes().request("OK"))
            );

            assertSame(err.toString(), MessagingRemoteException.class, err.getCause().getClass());
            assertTrue(ErrorUtils.stackTrace(err).contains(NotSerializableException.class.getName() + ": " + Socket.class.getName()));
        });
    }

    @Test
    public void testNonDeserializableResponse() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver(msg ->
            msg.reply(new NonDeserializable())
        );

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test").forRemotes().request("OK"))
            );

            assertSame(err.toString(), MessagingException.class, err.getCause().getClass());
            assertTrue(ErrorUtils.stackTrace(err).contains(InvalidClassException.class.getName() + ": " + HekateTestError.MESSAGE));
        });
    }

    private HekateTestNode prepareObjectSenderAndReceiver(MessageReceiver<Object> receiver) throws Exception {
        createNode(boot -> boot.withService(MessagingServiceFactory.class, f -> {
            f.withChannel(MessagingChannelConfig.of(Object.class)
                .withName("test")
                .withReceiver(receiver)
            );
        })).join();

        return createNode(boot -> boot.withService(MessagingServiceFactory.class, f -> {
            f.withChannel(MessagingChannelConfig.of(Object.class)
                .withName("test")
            );
        })).join();
    }

    private Throwable replyAndGetError(Message<String> reply) throws Exception {
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        runAsync(() -> {
            reply.reply("reply", errFuture::complete);

            return null;
        });

        return get(errFuture);
    }
}
