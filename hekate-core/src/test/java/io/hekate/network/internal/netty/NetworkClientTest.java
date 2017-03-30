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
import io.hekate.core.internal.util.Utils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.internal.NetworkClientCallbackMock;
import io.hekate.network.internal.NetworkSendCallbackMock;
import io.hekate.network.internal.NetworkServer;
import io.hekate.network.internal.NetworkServerCallbackMock;
import io.hekate.network.internal.NetworkServerHandlerMock;
import io.hekate.network.internal.NetworkTestBase;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkClientTest extends NetworkTestBase {
    public NetworkClientTest(HekateTestContext textContext) {
        super(textContext);
    }

    @Test
    public void testConnectToUnknown() throws Exception {
        NetworkClient<String> client = createClient(f -> f.setConnectTimeout(Integer.MAX_VALUE));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            try {
                client.connect(new InetSocketAddress(InetAddress.getLocalHost(), 15001), callback).get();

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectException.class));
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());
            assertNull(client.getRemoteAddress());
            assertNull(client.getLocalAddress());
            callback.assertConnects(0);
            callback.assertDisconnects(0);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
        });
    }

    @Test
    public void testConnectTimeoutFailure() throws Exception {
        NetworkClient<String> client = createClient(f -> f.setConnectTimeout(1));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            try {
                client.connect(new InetSocketAddress("hekate.io", 81), callback).get(3, TimeUnit.SECONDS);

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectTimeoutException.class));
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());
            assertNull(client.getRemoteAddress());
            assertNull(client.getLocalAddress());
            callback.assertConnects(0);
            callback.assertDisconnects(0);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectTimeoutException));
        });
    }

    @Test
    public void testConnectDisconnect() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            assertNotNull(client.getRemoteAddress());
            assertNotNull(client.getLocalAddress());
            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            listener.assertNoErrors();

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            assertNull(client.getRemoteAddress());
            assertNull(client.getLocalAddress());
            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
            listener.assertNoErrors();
        });
    }

    @Test
    public void testConnectWhileDisconnecting() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(5, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            assertNotNull(client.getRemoteAddress());
            assertNotNull(client.getLocalAddress());

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            listener.assertNoErrors();

            NetworkFuture<String> disconnect = client.disconnect();

            client.connect(server.getAddress(), callback).get();

            disconnect.get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            assertNotNull(client.getRemoteAddress());
            assertNotNull(client.getLocalAddress());

            callback.assertConnects(2);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            listener.assertNoErrors();

            client.disconnect().get();
        });
    }

    @Test
    public void testConnectWithPayload() throws Exception {
        NetworkServerHandlerMock<String> serverConnectionListener = new NetworkServerHandlerMock<>();

        NetworkServer server = createServer(serverConnectionListener);

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            client.connect(server.getAddress(), "test", callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            assertNotNull(client.getRemoteAddress());
            assertNotNull(client.getLocalAddress());
            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            serverConnectionListener.awaitForConnect(client);

            assertEquals(1, serverConnectionListener.getConnectPayload(client).size());
            assertEquals("test", serverConnectionListener.getConnectPayload(client).get(0));

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            assertNull(client.getRemoteAddress());
            assertNull(client.getLocalAddress());
            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            serverConnectionListener.reset();
        });
    }

    @Test
    public void testContext() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.setContext(i);

            client.connect(server.getAddress(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            assertEquals((Integer)i, client.getContext());

            client.disconnect().get();

            assertEquals((Integer)i, client.getContext());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testHeartbeats() throws Exception {
        int hbInterval = 100;
        int hbLossThreshold = 3;

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.getAddress(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            sleep(hbInterval * hbLossThreshold * 5);

            assertSame(NetworkClient.State.CONNECTED, client.getState());
            assertEquals(0, callback.getMessages().size());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testHeartbeatsWithData() throws Exception {
        int hbInterval = 100;
        int hbLossThreshold = 3;

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.getAddress(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            repeat(hbLossThreshold + 5, h -> {
                assertSame(NetworkClient.State.CONNECTED, client.getState());

                client.send("A");

                sleep(hbInterval);
            });

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            sleep(hbInterval * hbLossThreshold * 3);

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testSenderHeartbeatTimeout() throws Exception {
        int hbInterval = 100;
        int hbLossThreshold = 3;

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
            f.setDisableHeartbeats(true);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.getAddress(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            sleep(hbInterval * hbLossThreshold * 2);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());
            assertEquals(0, callback.getMessages().size());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertMaxErrors(1);
        });
    }

    @Test
    public void testReceiverHeartbeatTimeout() throws Exception {
        int hbInterval = 100;
        int hbLossThreshold = 3;

        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch disconnect = new CountDownLatch(1);

        AtomicReference<Throwable> errRef = new AtomicReference<>();
        Exchanger<NetworkEndpoint<String>> receiverRef = new Exchanger<>();

        // Register a callback that will give us back a reference to the receiver endpoint.
        NetworkServer server = createAndConfigureServerHandler(
            h -> h.setHandler((msg, from) -> {
                // Send a reply to trigger blocking on the sender side.
                from.send("reply");

                try {
                    receiverRef.exchange(from);
                } catch (InterruptedException e) {
                    errRef.compareAndSet(null, e);
                }
            }),
            s -> {
                s.setHeartbeatInterval(hbInterval);
                s.setHeartbeatLossThreshold(hbLossThreshold);
            }
        );

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        // Connect with blocking on first response from server.
        client.connect(server.getAddress(), new NetworkClientCallbackMock<String>() {
            @Override
            public void onMessage(NetworkMessage<String> message, NetworkClient<String> from) throws IOException {
                super.onMessage(message, client);

                // Block on I/O thread. This should trigger timeout on the receiver side.
                try {
                    resume.await();
                } catch (InterruptedException e) {
                    errRef.compareAndSet(null, e);
                }
            }

            @Override
            public void onDisconnect(NetworkClient<String> from) {
                super.onDisconnect(client);

                disconnect.countDown();
            }
        }).get();

        // Send a message to trigger response.
        client.send("test", new NetworkSendCallbackMock<>());

        NetworkEndpoint<String> receiver = receiverRef.exchange(null, 3, TimeUnit.SECONDS);

        assertNotNull(receiver);

        // Sleep for a while to make sure that the receiver side will close connection by timeout
        sleep(hbInterval * hbLossThreshold * 3);

        // Unblock sender.
        resume.countDown();

        // Receiver should initiate disconnect by timeout.
        await(disconnect);

        server.stop().get();
    }

    @Test
    public void testIdleTimeout() throws Exception {
        int hbInterval = 100;
        int hbLossThreshold = 3;

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient(f -> f.setIdleTimeout(hbInterval * hbLossThreshold * 2));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.getAddress(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);

            sleep(hbInterval * hbLossThreshold);

            callback.assertNoErrors();
            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.send("A");

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            sleep(hbInterval * hbLossThreshold * 3);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());
            assertEquals(0, callback.getMessages().size());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertMaxErrors(1);
        });
    }

    @Test
    public void testMultipleConnect() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            expect(IllegalStateException.class, () -> client.connect(server.getAddress(), new NetworkClientCallbackMock<>()));
            expect(IllegalStateException.class, () -> client.connect(server.getAddress(), new NetworkClientCallbackMock<>()));
            expect(IllegalStateException.class, () -> client.connect(server.getAddress(), new NetworkClientCallbackMock<>()));

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            listener.assertNoErrors();
        });
    }

    @Test
    public void testMultipleEnsureConnected() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            NetworkFuture<String> future = client.ensureConnected(server.getAddress(), callback);

            future.get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            NetworkClientCallbackMock<String> rejected1 = new NetworkClientCallbackMock<>();
            NetworkClientCallbackMock<String> rejected2 = new NetworkClientCallbackMock<>();
            NetworkClientCallbackMock<String> rejected3 = new NetworkClientCallbackMock<>();

            assertSame(future, client.ensureConnected(server.getAddress(), rejected1));
            assertSame(future, client.ensureConnected(server.getAddress(), rejected2));
            assertSame(future, client.ensureConnected(server.getAddress(), rejected3));

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            rejected1.assertConnects(0);
            rejected2.assertConnects(0);
            rejected3.assertConnects(0);

            listener.assertNoErrors();
        });
    }

    @Test
    public void testMultipleDisconnects() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            client.disconnect();
            client.disconnect();
            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            listener.assertNoErrors();
        });
    }

    @Test
    public void testCloseable() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            try (NetworkClient<String> closeable = client) {
                assertSame(NetworkClient.State.DISCONNECTED, closeable.getState());

                closeable.connect(server.getAddress(), callback).get();

                assertSame(NetworkClient.State.CONNECTED, closeable.getState());
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            listener.assertNoErrors();
        });
    }

    @Test
    public void testDisconnectOnServerStop() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            server.start(newServerAddress(), listener).get();

            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());

            server.stop().get();

            callback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            listener.assertNoErrors();
        });
    }

    @Test
    public void testDisconnectOnConnect() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        CountDownLatch disconnectLatch = new CountDownLatch(1);
        AtomicReference<NetworkFuture<String>> disconnectFuture = new AtomicReference<>();

        NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<String>() {
            @Override
            public void onConnect(NetworkClient<String> conn) {
                super.onConnect(conn);

                disconnectFuture.set(conn.disconnect());

                disconnectLatch.countDown();
            }
        };

        client.connect(server.getAddress(), callback).get();

        await(disconnectLatch);

        disconnectFuture.get().get();

        assertSame(NetworkClient.State.DISCONNECTED, client.getState());

        callback.assertConnects(1);
        callback.assertDisconnects(1);
        callback.assertErrors(0);
    }

    @Test
    public void testDisconnectNoWait() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress()).get();

        for (int i = 0; i < 10; i++) {
            for (int j = 20; j >= 0; j--) {
                NetworkClient<String> client = createClient();

                NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

                NetworkFuture<String> connect = client.connect(server.getAddress(), callback);

                if (j > 0) {
                    sleep(j);
                }

                NetworkFuture<String> disconnect = client.disconnect();

                disconnect.get();
                connect.get();

                assertSame(NetworkClient.State.DISCONNECTED, client.getState());

                callback.assertErrors(0);
            }
        }
    }

    @Test
    public void testWrongProtocol() throws Exception {
        NetworkServer server = createServer(createHandler("WRONG_PROTOCOL", (msg, from) -> {
            // No-op.
        }));

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

        try {
            client.connect(server.getAddress(), callback).get(3, TimeUnit.SECONDS);

            fail("Error was expected");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectException.class));
        }

        assertSame(NetworkClient.State.DISCONNECTED, client.getState());

        callback.assertConnects(0);
        callback.assertDisconnects(0);
        callback.assertErrors(1);
        callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
    }

    @Test
    public void testWrongProtocolWithDeferredMessages() throws Exception {
        NetworkServer server = createServer(createHandler("WRONG_PROTOCOL", (msg, from) -> {
            // No-op.
        }));

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(5, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            NetworkFuture<String> connect = client.connect(server.getAddress(), callback);

            NetworkSendCallbackMock<String> messageCallback = new NetworkSendCallbackMock<>();

            for (int j = 0; j < i + 1; j++) {
                client.send("msg-" + j + '-' + i, messageCallback);
            }

            try {
                connect.get();

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectException.class));
            }

            messageCallback.awaitForSentOrFailed(i + 1);

            messageCallback.assertSent(0);
            messageCallback.assertFailed(i + 1);

            assertSame(NetworkClient.State.DISCONNECTED, client.getState());

            callback.assertConnects(0);
            callback.assertDisconnects(0);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
        });
    }

    @Test
    public void testConnectFailureAfterHandlerUnregister() throws Exception {
        NetworkServerHandlerMock<String> serverListener = new NetworkServerHandlerMock<>();

        NetworkServer server = createServer(serverListener);

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

        client.connect(server.getAddress(), callback).get();

        assertSame(NetworkClient.State.CONNECTED, client.getState());

        client.send("test");

        serverListener.awaitForMessages(client.getLocalAddress(), "test");

        client.disconnect().get();

        assertSame(NetworkClient.State.DISCONNECTED, client.getState());

        server.removeHandler(TEST_PROTOCOL);

        try {
            client.connect(server.getAddress(), callback).get();

            fail("Error was expected");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectException.class));
        }

        assertSame(NetworkClient.State.DISCONNECTED, client.getState());

        callback.assertConnects(1);
        callback.assertDisconnects(1);
        callback.assertErrors(1);
        callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
    }

    @Test
    public void testTimeoutOnHandshake() throws Exception {
        CountDownLatch stopLatch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        NioEventLoopGroup serverGroup = new NioEventLoopGroup(2);

        Channel channel = null;

        try {
            // Prepare fake TCP server.
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(serverGroup, serverGroup);
            bootstrap.channel(NioServerSocketChannel.class);
            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    try {
                        // Hang on connect.
                        await(stopLatch);
                    } catch (Throwable e) {
                        error.set(e);
                    }
                }
            });

            InetSocketAddress addr = newNode().getSocket();

            channel = bootstrap.bind(addr).sync().channel();

            // Prepare client.
            NetworkClient<String> client = createClient(f -> f.setConnectTimeout(200));

            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();
            NetworkSendCallbackMock<String> messageCallback = new NetworkSendCallbackMock<>();

            sayTime("Connected", () -> {
                try {
                    NetworkFuture<String> future = client.connect(addr, callback);

                    client.send("one", messageCallback);
                    client.send("two", messageCallback);
                    client.send("three", messageCallback);

                    future.get(3, TimeUnit.SECONDS);

                    fail("Error was expected");
                } catch (ExecutionException e) {
                    assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectTimeoutException.class));
                }
            });

            callback.assertConnects(0);
            callback.assertErrors(1);

            messageCallback.assertFailed("one");
            messageCallback.assertFailed("two");
            messageCallback.assertFailed("three");

            callback.getErrors().forEach(e -> assertTrue(e instanceof ConnectTimeoutException));
        } finally {
            stopLatch.countDown();

            if (channel != null) {
                channel.close().await();
            }

            serverGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS).await().awaitUninterruptibly();

            assertNull(error.get());
        }
    }

    @Test
    public void testConnectIsNotBlocking() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        NetworkClient<String> badClient = createClient();
        NetworkClient<String> goodClient = createClient();

        server.start(newServerAddress(), listener).get();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> badCallback = new NetworkClientCallbackMock<>();
            NetworkClientCallbackMock<String> goodCallback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, badClient.getState());
            assertSame(NetworkClient.State.DISCONNECTED, goodClient.getState());

            NetworkFuture<String> badFuture = badClient.connect(new InetSocketAddress("localhost", 15000 + i), badCallback);

            NetworkFuture<String> goodFuture = goodClient.connect(server.getAddress(), goodCallback);

            sayTime("Good connect", goodFuture::get);
            sayTime("Bad connect", () -> {
                try {
                    badFuture.get();

                    fail("Error was expected.");
                } catch (ExecutionException e) {
                    assertTrue(e.getCause().toString(), Utils.isCausedBy(e, ConnectException.class));
                }
            });

            assertSame(NetworkClient.State.CONNECTED, goodClient.getState());
            assertSame(NetworkClient.State.DISCONNECTED, badClient.getState());

            badCallback.assertConnects(0);
            badCallback.assertDisconnects(0);
            badCallback.assertErrors(1);

            goodClient.disconnect().get();

            goodCallback.assertConnects(1);
            goodCallback.assertDisconnects(1);
            goodCallback.assertNoErrors();

            listener.assertNoErrors();
        });
    }
}
