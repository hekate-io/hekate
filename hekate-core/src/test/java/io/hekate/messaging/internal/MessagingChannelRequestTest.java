package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.internal.util.Waiting;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.UnknownRouteException;
import io.hekate.messaging.unicast.LoadBalancingException;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.TooManyRoutesException;
import io.hekate.network.NetworkFuture;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingChannelRequestTest extends MessagingServiceTestBase {
    public MessagingChannelRequestTest(MessagingTestContext ctx) {
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
                    channel.get().request("" + i).response(3, TimeUnit.SECONDS);
                } catch (MessagingFutureException e) {
                    assertTrue(getStacktrace(e), e.getCause() instanceof TooManyRoutesException);
                }

                responses.add(channel.get().forOldest().request("" + i).response(3, TimeUnit.SECONDS));

                assertEquals(1, responses.size());
            }
        }
    }

    @Test
    public void testCallback() throws Exception {
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
    public void testFuture() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

            c.setReceiver(receiver);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.getNodeId();
                String msg2 = "test2-" + from.getNodeId();

                assertEquals(msg1 + "-reply", from.get().forNode(to.getNodeId()).request(msg1).response());
                assertEquals(msg2 + "-reply", from.get().forNode(to.getNodeId()).request(msg2).responseUninterruptedly());

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

            Response<String> msg = sender.requestWithSyncCallback(receiver.getNodeId(), "request");

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
                    String msg = "test-" + from.getNodeId();

                    Thread.currentThread().interrupt();

                    String response = from.get().forNode(to.getNodeId()).request(msg).responseUninterruptedly();

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
    public void testUnknownNodeCallback() throws Exception {
        TestChannel channel = createChannel().join();

        channel.requestWithSyncCallback(newNodeId(), "failed");
    }

    @Test
    public void testUnknownNodeFuture() throws Exception {
        TestChannel channel = createChannel().join();

        try {
            channel.request(newNodeId(), "failed").get();

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue("" + e, e.isCausedBy(UnknownRouteException.class));
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idleTimeout)).join();

            TestChannel receiver = createChannel(c -> {
                c.setIdleTimeout(idleTimeout);
                c.setReceiver(msg -> msg.reply("ok"));
            }).join();

            awaitForChannelsTopology(sender, receiver);

            MessagingClient<String> client = sender.getImpl().getClient(receiver.getNodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                sender.request(receiver.getNodeId(), "test" + i).get();

                busyWait("disconnect idle", () -> !client.isConnected());

                assertFalse(client.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.getReceived().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testNoFailuresWithSmallIdleTimeout() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 1 + j;

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idleTimeout)).join();

            TestChannel receiver = createChannel(c -> {
                c.setIdleTimeout(idleTimeout);
                c.setReceiver(msg -> msg.reply("ok"));
            }).join();

            awaitForChannelsTopology(sender, receiver);

            runParallel(4, 2500, s ->
                sender.request(receiver.getNodeId(), "test").get()
            );

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testIdleTimeoutWithPendingResponse() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleTimeout(idleTimeout)).join();

            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            TestChannel receiver = createChannel(c -> {
                c.setIdleTimeout(idleTimeout);
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

            MessagingClient<String> client = sender.getImpl().getClient(receiver.getNodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                String request = "test" + i;

                ResponseCallbackMock callback = new ResponseCallbackMock(request);

                latchRef.set(new CountDownLatch(1));

                try {
                    // Send message.
                    sender.request(receiver.getNodeId(), request, callback);

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
            assertEquals(5, receiver.getReceived().size());

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
                sender.requestWithSyncCallback(receiver.getNodeId(), msg);

                fail("Error was expected.");
            } catch (ClosedChannelException e) {
                // No-op.
            }

            receiver.awaitForMessage(msg);
        });
    }

    @Test
    public void testNetworkDisconnectWhileSending() throws Throwable {
        TestChannel sender = createChannel().join();
        TestChannel receiver = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        MessagingClient<String> client = sender.getImpl().getClient(receiver.getNodeId());

        List<NetworkFuture<MessagingProtocol>> closeFuture = client.close();

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
    public void testNetworkDisconnectWhileReceiving() throws Throwable {
        TestChannel sender = createChannel().join();

        AtomicReference<MessagingClient<String>> clientRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c ->
            c.withReceiver(msg ->
                clientRef.get().close()
            )
        ).join();

        awaitForChannelsTopology(sender, receiver);

        MessagingClient<String> client = sender.getImpl().getClient(receiver.getNodeId());

        clientRef.set(client);

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

                return receiver.getNodeId();
            }).request("test"));

            await(routeLatch);

            Waiting close = sender.getImpl().close();

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
            ResponseCallbackMock leaveCallback = new ResponseCallbackMock(leaveMsg);

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
        ResponseCallbackMock toSelf = new ResponseCallbackMock("to-self");
        ResponseCallbackMock toRemote = new ResponseCallbackMock("to-remote");

        channel.getNode().cluster().addListener(event -> {
            try {
                MessagingChannel<String> send = channel.getNode().messaging().channel(TEST_CHANNEL_NAME);

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
    public void testLoadBalanceFailure() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(3, i -> {
            ResponseFuture<String> future = sender.withLoadBalancer((msg, topology) -> {
                throw new TestHekateException(TEST_ERROR_MESSAGE);
            }).request("failed" + i);

            try {
                future.response();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(TestHekateException.class));
                assertEquals(TEST_ERROR_MESSAGE, e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").response());
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
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(LoadBalancingException.class));
                assertEquals("Load balancer failed to select a target node.", e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").response());
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
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(UnknownRouteException.class));
                assertEquals("Node is not within the channel topology [id=" + invalidNodeId + ']', e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", sender.request(receiver.getNodeId(), "success").response());
    }

    @Test
    public void testTooManyRoutes() throws Throwable {
        MessageReceiver<String> receiver = msg -> {
            String response = msg.get() + "-reply";

            msg.reply(response);
        };

        TestChannel channel1 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel2 = createChannel(c -> c.setReceiver(receiver)).join();

        awaitForChannelsTopology(channel1, channel2);

        repeat(3, i -> {
            ResponseFuture<String> future = channel1.get().request("failed" + i);

            try {
                future.response();
            } catch (MessagingFutureException e) {
                assertTrue(e.isCausedBy(TooManyRoutesException.class));
                assertEquals("Too many receivers "
                        + "[channel=" + channel1.get().getName() + ", topology=" + channel1.get().getCluster().getTopology() + ']',
                    e.getCause().getMessage());
            }
        });

        assertEquals("success-reply", channel1.request(channel2.getNodeId(), "success").response());
    }

    @Test
    public void testNoReceiver() throws Exception {
        TestChannel channel = createChannel(c ->
            c.withClusterFilter(n -> !n.isLocal())
        ).join();

        try {
            get(channel.request(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof LoadBalancingException);
            assertEquals("No suitable receivers [channel=test_channel]", e.getCause().getMessage());
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
            get(channel.request(channel.getNodeId(), "test"));
        } catch (MessagingFutureException e) {
            assertTrue(e.getCause().toString(), e.getCause() instanceof MessagingChannelClosedException);
            assertEquals("Channel closed [channel=test_channel]", e.getCause().getMessage());
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
}
