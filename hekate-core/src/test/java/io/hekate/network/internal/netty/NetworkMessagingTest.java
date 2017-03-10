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

package io.hekate.network.internal.netty;

import io.hekate.HekateTestContext;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkTransportType;
import io.hekate.network.internal.NetworkClientCallbackMock;
import io.hekate.network.internal.NetworkSendCallbackMock;
import io.hekate.network.internal.NetworkServer;
import io.hekate.network.internal.NetworkServerHandlerMock;
import io.hekate.network.internal.NetworkTestBase;
import io.netty.channel.EventLoopGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class NetworkMessagingTest extends NetworkTestBase {
    public static class NetworkMessagingTestContext extends HekateTestContext {
        private final int handlerThreads;

        public NetworkMessagingTestContext(NetworkTransportType transport, int handlerThreads) {
            super(transport);

            this.handlerThreads = handlerThreads;
        }
    }

    private final int handlerThreads;

    private NetworkServerHandlerMock<String> serverHandler;

    private NetworkServer server;

    private NetworkClient<String> client;

    private NetworkClientCallbackMock<String> clientCallback;

    private NetworkSendCallbackMock<String> messageCallback;

    private EventLoopGroup handlerEventLoopGroup;

    public NetworkMessagingTest(NetworkMessagingTestContext ctx) {
        super(ctx);

        this.handlerThreads = ctx.handlerThreads;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<NetworkMessagingTestContext> getNetworkMessagingTestContexts() {
        return getNetworkTestContexts().stream().flatMap(ctx ->
            Stream.of(
                new NetworkMessagingTestContext(ctx.getTransport(), 0),
                new NetworkMessagingTestContext(ctx.getTransport(), 1),
                new NetworkMessagingTestContext(ctx.getTransport(), 2)
            )
        ).collect(toList());
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        serverHandler = new NetworkServerHandlerMock<>();

        NettyServerHandlerConfig<String> handlerCfg = createHandler(serverHandler);

        if (handlerThreads > 0) {
            handlerEventLoopGroup = newEventLoop(handlerThreads);

            handlerCfg.setEventLoopGroup(handlerEventLoopGroup);
        }

        server = createServer(handlerCfg);

        server.start(newServerAddress()).get();

        client = createClient();

        clientCallback = new NetworkClientCallbackMock<>();

        messageCallback = new NetworkSendCallbackMock<>();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (handlerEventLoopGroup != null) {
                handlerEventLoopGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
            }
        }
    }

    @Test
    public void testSend() throws Exception {
        repeat(3, i -> {
            client.connect(server.getAddress(), clientCallback).get();

            client.send("one");
            client.send("two");
            client.send("three");

            serverHandler.awaitForMessages(client, "one", "two", "three");

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendWithCallback() throws Exception {
        repeat(3, i -> {
            client.connect(server.getAddress(), clientCallback).get();

            client.send("one", messageCallback);
            client.send("two", messageCallback);
            client.send("three", messageCallback);

            serverHandler.awaitForMessages(client, "one", "two", "three");

            messageCallback.awaitForSent("one", "two", "three");

            client.disconnect().get();

            serverHandler.reset();
            messageCallback.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendFailureIfDisconnected() throws Exception {
        repeat(5, i -> {
            client.send("one", messageCallback);
            client.send("two", messageCallback);
            client.send("three", messageCallback);

            client.connect(server.getAddress(), clientCallback).get();

            client.send("four", messageCallback);

            serverHandler.awaitForMessages(client, "four");

            serverHandler.assertNotReceived(client, "one", "two", "three");

            messageCallback.assertFailed("one");
            messageCallback.assertFailed("two");
            messageCallback.assertFailed("three");

            messageCallback.awaitForSent("four");

            client.disconnect().get();

            serverHandler.reset();
            messageCallback.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendOnConnect() throws Exception {
        repeat(3, i -> {
            client.connect(server.getAddress(), new NetworkClientCallback<String>() {
                @Override
                public void onConnect(NetworkClient<String> client) {
                    client.send("one");
                    client.send("two");
                    client.send("three");
                }

                @Override
                public void onMessage(NetworkMessage<String> message, NetworkClient<String> client) {
                    // No-op.
                }
            }).get();

            serverHandler.awaitForMessages(client, "one", "two", "three");

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendOnConnectDeferredOrder() throws Exception {
        repeat(3, i -> {
            CountDownLatch onConnect = new CountDownLatch(1);
            CountDownLatch onDeferredSent = new CountDownLatch(1);

            NetworkFuture<String> connect = client.connect(server.getAddress(), new NetworkClientCallback<String>() {
                @Override
                public void onConnect(NetworkClient<String> client) {
                    onConnect.countDown();

                    await(onDeferredSent);

                    client.send("one");
                    client.send("two");
                    client.send("three");
                }

                @Override
                public void onMessage(NetworkMessage<String> message, NetworkClient<String> client) {
                    // No-op.
                }
            });

            await(onConnect);

            client.send("1");
            client.send("2");
            client.send("3");

            onDeferredSent.countDown();

            connect.get();

            serverHandler.awaitForMessages(client, "1", "2", "3", "one", "two", "three");

            List<String> messages = serverHandler.getMessages(client);

            assertEquals("1", messages.get(0));
            assertEquals("2", messages.get(1));
            assertEquals("3", messages.get(2));
            assertEquals("one", messages.get(3));
            assertEquals("two", messages.get(4));
            assertEquals("three", messages.get(5));

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendDeferred() throws Exception {
        repeat(3, i -> {
            NetworkFuture<String> future = client.connect(server.getAddress(), clientCallback);

            client.send("one");
            client.send("two");
            client.send("three");

            future.get();

            serverHandler.awaitForMessages(client, "one", "two", "three");

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testDiscardDeferred() throws Exception {
        int port = newTcpPort();

        try (ServerSocket blockedServer = new ServerSocket(port)) {
            repeat(3, i -> {
                assertFalse(blockedServer.isClosed());

                NetworkSendCallbackMock<String> callback = new NetworkSendCallbackMock<>();

                NetworkFuture<String> connFuture = client.connect(new InetSocketAddress(InetAddress.getLocalHost(), port), clientCallback);

                client.send("one", callback);
                client.send("two", callback);
                client.send("three", callback);

                NetworkFuture<String> discFuture = client.disconnect();

                connFuture.get();
                discFuture.get();

                callback.assertFailed("one");
                callback.assertFailed("two");
                callback.assertFailed("three");

                clientCallback.reset();
            });
        }
    }

    @Test
    public void testReply() throws Exception {
        repeat(3, i -> {
            String[] responses1 = new String[i];
            String[] responses2 = new String[i];
            String[] responses3 = new String[i];

            for (int j = 0; j < i; j++) {
                responses1[j] = "responseOne" + j;
                responses2[j] = "responseTwo" + j;
                responses3[j] = "responseThree" + j;
            }

            serverHandler.addReplyWith("one", responses1);
            serverHandler.addReplyWith("two", responses2);
            serverHandler.addReplyWith("three", responses3);

            client.connect(server.getAddress(), clientCallback).get();

            client.send("one");
            client.send("two");
            client.send("three");

            serverHandler.awaitForMessages(client, "one", "two", "three");

            clientCallback.awaitForMessagesBatch(responses1, responses2, responses3);

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testReplyOnConnect() throws Exception {
        repeat(3, i -> {
            serverHandler.addSendOnConnect("one", "two", "tree");

            client.connect(server.getAddress(), clientCallback).get();

            clientCallback.awaitForMessages("one", "two", "tree");

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testReplyWithCallbackSuccess() throws Exception {
        repeat(3, i -> {
            NetworkSendCallbackMock<String> serverCallback = new NetworkSendCallbackMock<>();

            serverHandler.addReplyWith("one", serverCallback, "serverOne");
            serverHandler.addReplyWith("two", serverCallback, "serverTwo");
            serverHandler.addReplyWith("three", serverCallback, "serverThree");

            client.connect(server.getAddress(), clientCallback).get();

            client.send("one");
            client.send("two");
            client.send("three");

            serverHandler.awaitForMessages(client, "one", "two", "three");

            clientCallback.awaitForMessages("serverOne", "serverTwo", "serverThree");

            serverCallback.assertSent("serverOne");
            serverCallback.assertSent("serverTwo");
            serverCallback.assertSent("serverThree");
            serverCallback.assertFailed(0);

            serverHandler.assertConnects(client, 1);

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testReplyWithCallbackFailure() throws Exception {
        repeat(3, i -> {
            NetworkSendCallbackMock<String> serverCallback = new NetworkSendCallbackMock<>();

            serverHandler.addDisconnectOnMessage("one");
            serverHandler.addReplyWith("one", serverCallback, "serverOne", "serverTwo", "serverThree");

            client.connect(server.getAddress(), clientCallback).get();

            InetSocketAddress address = client.getLocalAddress();

            client.send("one");

            serverHandler.awaitForMessages(address, "one");

            serverCallback.awaitForErrors("serverOne", "serverTwo", "serverThree");

            assertEquals(0, clientCallback.getMessages().size());

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testDisconnectOnFirstMessage() throws Exception {
        repeat(3, i -> {
            serverHandler.addDisconnectOnMessage("one");

            client.connect(server.getAddress(), clientCallback).get();

            InetSocketAddress address = client.getLocalAddress();

            client.send("one");
            client.send("two");
            client.send("three");

            serverHandler.awaitForMessages(address, "one");

            serverHandler.assertConnects(address, 1);

            clientCallback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testDisconnectOnFirstMessageDeferred() throws Exception {
        repeat(3, i -> {
            serverHandler.addDisconnectOnMessage("one");

            NetworkFuture<String> connect = client.connect(server.getAddress(), clientCallback);

            client.send("one");
            client.send("two");
            client.send("three");

            connect.get();

            serverHandler.awaitForMessages(clientCallback.getLastLocalAddress(), "one");

            serverHandler.assertConnects(clientCallback.getLastLocalAddress(), 1);

            clientCallback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testDisconnectOnLastMessageDeferred() throws Exception {
        repeat(3, i -> {
            serverHandler.addDisconnectOnMessage("three");

            NetworkFuture<String> connect = client.connect(server.getAddress(), clientCallback);

            client.send("one");
            client.send("two");
            client.send("three");

            connect.get();

            serverHandler.awaitForMessages(clientCallback.getLastLocalAddress(), "one", "two", "three");

            serverHandler.assertConnects(clientCallback.getLastLocalAddress(), 1);

            clientCallback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testDisconnectOnLastMessage() throws Exception {
        repeat(3, i -> {
            serverHandler.addDisconnectOnMessage("three");

            client.connect(server.getAddress(), clientCallback).get();

            InetSocketAddress address = client.getLocalAddress();

            client.send("one");
            client.send("two");
            client.send("three");

            serverHandler.awaitForMessages(address, "one", "two", "three");

            serverHandler.assertConnects(address, 1);

            clientCallback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendAfterReconnectNoWait() throws Exception {
        repeat(3, i -> {
            CountDownLatch latch = new CountDownLatch(1);

            client.connect(server.getAddress(), new NetworkClientCallback<String>() {
                @Override
                public void onConnect(NetworkClient<String> client) {
                    client.disconnect();

                    latch.countDown();
                }

                @Override
                public void onMessage(NetworkMessage<String> message, NetworkClient<String> client) {
                    // No-op.
                }
            });

            await(latch);

            NetworkFuture<String> connect = client.connect(server.getAddress(), clientCallback);

            NetworkSendCallbackMock<String> messageCallback = new NetworkSendCallbackMock<>();

            client.send("one", messageCallback);
            client.send("two", messageCallback);
            client.send("three", messageCallback);

            connect.get();

            serverHandler.awaitForMessages(client, "one", "two", "three");

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }

    @Test
    public void testSendAfterReconnectNoWaitWithDeferred() throws Exception {
        repeat(3, i -> {
            client.connect(server.getAddress(), new NetworkClientCallback<String>() {
                @Override
                public void onConnect(NetworkClient<String> client) {
                    client.disconnect();
                }

                @Override
                public void onMessage(NetworkMessage<String> message, NetworkClient<String> client) {
                    // No-op.
                }
            });

            NetworkSendCallbackMock<String> msgCallback = new NetworkSendCallbackMock<>();

            client.send("A", msgCallback);
            client.send("B", msgCallback);
            client.send("C", msgCallback);

            msgCallback.awaitForErrors("A", "B", "C");

            assertTrue("" + msgCallback.getFailure("A"), msgCallback.getFailure("A") instanceof ClosedChannelException);
            assertTrue("" + msgCallback.getFailure("B"), msgCallback.getFailure("B") instanceof ClosedChannelException);
            assertTrue("" + msgCallback.getFailure("C"), msgCallback.getFailure("C") instanceof ClosedChannelException);

            NetworkFuture<String> connect = client.connect(server.getAddress(), clientCallback);

            client.send("one", msgCallback);
            client.send("two", msgCallback);
            client.send("three", msgCallback);

            connect.get();

            serverHandler.awaitForMessages(client, "one", "two", "three");

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect().get();

            serverHandler.reset();
            clientCallback.reset();
        });
    }
}
