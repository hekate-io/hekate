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
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkService;
import io.hekate.network.internal.NetworkClientCallbackMock;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingChannelTest extends MessagingServiceTestBase {
    public MessagingChannelTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testChannel() throws Exception {
        TestChannel testChannel = createChannel().join();

        MessagingChannel<String> channel = testChannel.get();

        assertNotNull(channel.getId());
        assertEquals(TEST_CHANNEL_NAME, channel.getName());
        assertNotNull(channel.getCluster());
        assertThat(channel.getCluster().getTopology().getNodes(), hasItem(testChannel.getInstance().getNode()));
        assertEquals(nioThreads, channel.getNioThreads());
        assertEquals(workerThreads, channel.getWorkerThreads());
        assertNotNull(channel.getExecutor());
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
    public void testMessagingEndpoint() throws Exception {
        TestChannel sender = createChannel().join();

        AtomicReference<AssertionError> errorRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            try {
                MessagingEndpoint<String> endpoint = msg.getEndpoint();

                assertEquals(sender.getId(), endpoint.getRemoteId());

                endpoint.setContext("test");
                assertEquals("test", endpoint.getContext());

                endpoint.setContext(null);
                assertNull(endpoint.getContext());

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
    public void testMessageState() throws Exception {
        Exchanger<Throwable> errRef = new Exchanger<>();

        createChannel(c -> c.setReceiver(msg -> {
            try {
                if ("send".equals(msg.get()) || "broadcast".equals(msg.get())) {
                    assertFalse(msg.mustReply());
                    assertFalse(msg.isRequest());
                    assertFalse(msg.isStreamRequest());

                    assertResponseUnsupported(msg);
                } else if ("request".equals(msg.get()) || "aggregate".equals(msg.get())) {
                    assertTrue(msg.mustReply());
                    assertTrue(msg.isRequest());
                    assertFalse(msg.isStreamRequest());

                    msg.reply("ok");

                    assertFalse(msg.mustReply());
                    assertTrue(msg.isRequest());
                    assertFalse(msg.isStreamRequest());

                    assertResponded(msg);
                } else if ("stream-request".equals(msg.get())) {
                    assertTrue(msg.mustReply());
                    assertTrue(msg.isRequest());
                    assertTrue(msg.isStreamRequest());

                    for (int i = 0; i < 5; i++) {
                        msg.partialReply("ok");

                        assertTrue(msg.mustReply());
                        assertTrue(msg.isRequest());
                        assertTrue(msg.isStreamRequest());
                    }

                    msg.reply("ok");

                    assertFalse(msg.mustReply());
                    assertTrue(msg.isRequest());
                    assertTrue(msg.isStreamRequest());

                    assertResponded(msg);
                } else {
                    fail("Unexpected message: " + msg);
                }

                errRef.exchange(null);
            } catch (Throwable t) {
                try {
                    errRef.exchange(t);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        })).join();

        TestChannel sender = createChannel().join();

        sender.get().forRemotes().send("send");
        Optional.ofNullable(errRef.exchange(null)).ifPresent(e -> {
            throw new AssertionError(e);
        });

        sender.get().forRemotes().request("request");
        Optional.ofNullable(errRef.exchange(null)).ifPresent(e -> {
            throw new AssertionError(e);
        });

        sender.get().forRemotes().streamRequest("stream-request");
        Optional.ofNullable(errRef.exchange(null)).ifPresent(e -> {
            throw new AssertionError(e);
        });

        sender.get().forRemotes().broadcast("broadcast");
        Optional.ofNullable(errRef.exchange(null)).ifPresent(e -> {
            throw new AssertionError(e);
        });

        sender.get().forRemotes().aggregate("aggregate");
        Optional.ofNullable(errRef.exchange(null)).ifPresent(e -> {
            throw new AssertionError(e);
        });
    }
}
