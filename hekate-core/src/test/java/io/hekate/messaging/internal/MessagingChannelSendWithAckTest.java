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

import io.hekate.cluster.ClusterNode;
import io.hekate.codec.CodecException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.test.HekateTestError;
import io.hekate.test.NonDeserializable;
import io.hekate.test.NonSerializable;
import java.io.NotSerializableException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

public class MessagingChannelSendWithAckTest extends MessagingServiceTestBase {
    public MessagingChannelSendWithAckTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3);

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.nodeId();

                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                channel.newSend(msg).withAck().sync();

                to.assertReceived(msg);
            }
        }
    }

    @Test
    public void testUnknownNode() throws Exception {
        TestChannel channel = createChannel().join();

        try {
            channel.channel().forNode(newNodeId())
                .newSend("failed")
                .withAck()
                .sync();

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
            TestChannel receiver = createChannel(c -> c.setIdleSocketTimeout(idleTimeout)).join();

            awaitForChannelsTopology(sender, receiver);

            MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                sender.channel().forNode(receiver.nodeId())
                    .newSend("test" + i)
                    .withAck()
                    .sync();

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

        TestChannel receiver = createChannel(c -> c.setIdleSocketTimeout(50)).join();

        awaitForChannelsTopology(sender, receiver);

        runParallel(4, 1000, s ->
            sender.channel().forNode(receiver.nodeId())
                .newSend("test")
                .withAck()
                .sync()
        );

        sender.leave();
        receiver.leave();
    }

    @Test
    public void testIdleTimeoutWithInFlightMessage() throws Exception {
        repeat(3, j -> {
            int idleTimeout = 20 * (j + 1);

            TestChannel sender = createChannel(c -> c.setIdleSocketTimeout(idleTimeout)).join();

            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            TestChannel receiver = createChannel(c -> {
                c.setIdleSocketTimeout(idleTimeout);
                c.setReceiver(msg -> {
                    assertFalse(msg.mustReply());

                    await(latchRef.get());
                });
            }).join();

            awaitForChannelsTopology(receiver, sender);

            MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

            repeat(5, i -> {
                assertFalse(client.isConnected());

                SendFuture sent;

                latchRef.set(new CountDownLatch(1));

                try {
                    // Send message.
                    sent = sender.channel().forNode(receiver.nodeId())
                        .newSend("test" + i)
                        .withAck()
                        .submit();

                    // Await for timeout.
                    sleep((long)(idleTimeout * 3));

                    // Client must be still connected since there is an in-flight message.
                    assertTrue(client.isConnected());
                } finally {
                    // Trigger reply.
                    latchRef.get().countDown();
                }

                get(sent);

                // Pool must be disconnected since there are no in-flight messages.
                busyWait("pool disconnect", () -> !client.isConnected());
            });

            receiver.awaitForMessage("test4");
            assertEquals(5, receiver.received().size());

            sender.leave();
            receiver.leave();
        });
    }

    @Test
    public void testAffinityRouting() throws Throwable {
        Map<ClusterNode, List<String>> received = new ConcurrentHashMap<>();

        MessageReceiver<String> receiver = msg -> {
            ClusterNode localNode = msg.channel().cluster().topology().localNode();

            received.computeIfAbsent(localNode, n -> synchronizedList(new ArrayList<>())).add(msg.payload());
        };

        TestChannel channel1 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel2 = createChannel(c -> c.setReceiver(receiver)).join();
        TestChannel channel3 = createChannel(c -> c.setReceiver(receiver)).join();

        awaitForChannelsTopology(channel1, channel2, channel3);

        Set<ClusterNode> uniqueNodes = new HashSet<>();

        repeat(25, j -> {
            received.clear();

            for (int i = 0; i < 50; i++) {
                get(channel1.channel()
                    .newSend("test" + i)
                    .withAck()
                    .withAffinity(j)
                    .submit()
                );
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
    public void testReceiveFailure() throws Throwable {
        TestChannel sender = createChannel().join();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            throw TEST_ERROR;
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            String msg = "request" + i;

            try {
                sender.channel().forNode(receiver.nodeId())
                    .newSend(msg)
                    .withAck()
                    .sync();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                MessagingRemoteException remote = e.findCause(MessagingRemoteException.class);

                assertNotNull(e.toString(), remote);
                assertTrue(remote.remoteStackTrace().contains(HekateTestError.MESSAGE));
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

        client.close().forEach(CompletableFuture::join);

        repeat(5, i -> {
            try {
                sender.channel().forNode(receiver.nodeId())
                    .newSend("request" + i)
                    .withAck()
                    .sync();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
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
                clientRef.get().close().forEach(CompletableFuture::join)
            )
        ).join();

        awaitForChannelsTopology(sender, receiver);

        MessagingClient<String> client = sender.impl().clientOf(receiver.nodeId());

        clientRef.set(client);

        repeat(5, i -> {
            try {
                sender.channel().forNode(receiver.nodeId()).newSend("request" + i).withAck().sync();

                fail("Error was expected.");
            } catch (MessagingFutureException e) {
                // No-op.
            }
        });
    }

    @Test
    public void testNonSerializableMessage() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver();

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test")
                    .forRemotes()
                    .newSend(new NonSerializable())
                    .withAck()
                    .submit()
                )
            );

            assertSame(err.toString(), MessagingException.class, err.getCause().getClass());
            assertTrue(err.isCausedBy(CodecException.class));
            assertTrue(err.isCausedBy(NotSerializableException.class));
        });
    }

    @Test
    public void testNonDeSerializableMessage() throws Exception {
        HekateTestNode sender = prepareObjectSenderAndReceiver();

        repeat(5, i -> {
            MessagingFutureException err = expect(MessagingFutureException.class, () ->
                get(sender.messaging().channel("test")
                    .forRemotes()
                    .newSend(new NonDeserializable())
                    .withAck()
                    .submit()
                )
            );

            assertSame(err.toString(), MessagingRemoteException.class, err.getCause().getClass());
            assertTrue(ErrorUtils.stackTrace(err).contains(HekateTestError.MESSAGE));
        });
    }

    private HekateTestNode prepareObjectSenderAndReceiver() throws Exception {
        createNode(boot -> boot.withService(MessagingServiceFactory.class, f -> {
            f.withChannel(MessagingChannelConfig.of(Object.class)
                .withName("test")
                .withReceiver(msg -> {
                    // No-op.
                })
            );
        })).join();

        return createNode(boot -> boot.withService(MessagingServiceFactory.class, f -> {
            f.withChannel(MessagingChannelConfig.of(Object.class)
                .withName("test")
            );
        })).join();
    }
}
