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

package io.hekate.network.netty;

import io.hekate.HekateTestContext;
import io.hekate.codec.CodecException;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnectTimeoutException;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkSendCallbackMock;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerCallbackMock;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkServerHandlerMock;
import io.hekate.network.internal.NetworkTestBase;
import io.hekate.test.NetworkClientCallbackMock;
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
import java.net.ServerSocket;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;
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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            try {
                client.connect(new InetSocketAddress(InetAddress.getLocalHost(), 15001), callback).get();

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectException.class, e));
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.state());
            callback.assertConnects(0);
            callback.assertDisconnects(1);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
        });
    }

    @Test
    public void testSendToUnknown() throws Exception {
        NetworkClient<String> client = createClient(f -> f.setConnectTimeout(Integer.MAX_VALUE));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            NetworkFuture<String> future = client.connect(new InetSocketAddress(InetAddress.getLocalHost(), 15001), callback);

            NetworkSendCallbackMock<String> sendCallback = new NetworkSendCallbackMock<>();

            client.send("test", sendCallback);

            try {
                future.get();

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(IOException.class, e));
            }

            sendCallback.awaitForErrors("test");

            Throwable sendErr = sendCallback.getFailure("test");

            assertNotNull(sendErr);
            assertTrue(getStacktrace(sendErr), ErrorUtils.isCausedBy(IOException.class, sendErr));

            assertSame(NetworkClient.State.DISCONNECTED, client.state());
            callback.assertConnects(0);
            callback.assertDisconnects(1);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
        });
    }

    @Test
    public void testConnectTimeoutFailure() throws Exception {
        NetworkClient<String> client = createClient(f -> f.setConnectTimeout(1));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            try {
                get(client.connect(new InetSocketAddress("hekate.io", 81), callback));

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectTimeoutException.class, e));
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.state());
            callback.assertConnects(0);
            callback.assertDisconnects(1);
            callback.assertErrors(1);
            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof NetworkConnectTimeoutException));
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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            assertNotNull(client.remoteAddress());
            assertNotNull(client.localAddress());
            assertEquals(context().ssl().isPresent(), client.isSecure());
            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            listener.assertNoErrors();

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
            listener.assertNoErrors();
        });
    }

    @Test
    public void testThreadAffinity() throws Exception {
        Set<Thread> uniqueThreads = synchronizedSet(newSetFromMap(new IdentityHashMap<>()));

        NetworkServer server = createAndConfigureServerHandler(h -> h.withHandler(new NetworkServerHandler<String>() {
            @Override
            public void onConnect(String login, NetworkEndpoint<String> client) {
                uniqueThreads.add(Thread.currentThread());
            }

            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) throws IOException {
                uniqueThreads.add(Thread.currentThread());

                from.send("response");
            }
        }));

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(15, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), callback).get();

            client.send("test", new NetworkSendCallbackMock<>());

            callback.awaitForMessages("response");

            client.disconnect().get();

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

            assertEquals(1, uniqueThreads.size());
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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            assertNotNull(client.remoteAddress());
            assertNotNull(client.localAddress());

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            listener.assertNoErrors();

            NetworkFuture<String> disconnect = client.disconnect();

            client.connect(server.address(), callback).get();

            disconnect.get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            assertNotNull(client.remoteAddress());
            assertNotNull(client.localAddress());

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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            client.connect(server.address(), "test", callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            assertNotNull(client.remoteAddress());
            assertNotNull(client.localAddress());
            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            serverConnectionListener.awaitForConnect(client);

            assertEquals(1, serverConnectionListener.getConnectPayload(client).size());
            assertEquals("test", serverConnectionListener.getConnectPayload(client).get(0));

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

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

            client.connect(server.address(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            assertEquals(i, client.getContext());

            client.disconnect().get();

            assertEquals(i, client.getContext());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testHeartbeats() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            sleep(hbInterval * hbLossThreshold * 3);

            assertSame(NetworkClient.State.CONNECTED, client.state());
            assertEquals(0, callback.getMessages().size());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testHeartbeatsWithData() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            repeat(hbLossThreshold + 2, h -> {
                assertSame(NetworkClient.State.CONNECTED, client.state());

                client.send("A");

                sleep(hbInterval);
            });

            assertSame(NetworkClient.State.CONNECTED, client.state());

            sleep(hbInterval * hbLossThreshold * 3);

            assertSame(NetworkClient.State.CONNECTED, client.state());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();
        });
    }

    @Test
    public void testSenderHeartbeatTimeout() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
            f.setDisableHeartbeats(true);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);
            callback.assertNoErrors();

            sleep(hbInterval * hbLossThreshold * 2);

            assertSame(NetworkClient.State.DISCONNECTED, client.state());
            assertEquals(0, callback.getMessages().size());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertMaxErrors(1);
        });
    }

    @Test
    public void testReceiverHeartbeatTimeout() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

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
        client.connect(server.address(), new NetworkClientCallbackMock<String>() {
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
            public void onDisconnect(NetworkClient<String> from, Optional<Throwable> cause) {
                super.onDisconnect(client, cause);

                disconnect.countDown();
            }
        }).get();

        // Send a message to trigger response.
        client.send("test", new NetworkSendCallbackMock<>());

        NetworkEndpoint<String> receiver = receiverRef.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS);

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
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient(f -> f.setIdleTimeout(hbInterval * hbLossThreshold * 2));

        repeat(3, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), callback).get();

            callback.assertConnects(1);
            callback.assertDisconnects(0);

            sleep(hbInterval * hbLossThreshold);

            callback.assertNoErrors();
            assertSame(NetworkClient.State.CONNECTED, client.state());

            client.send("A");

            assertSame(NetworkClient.State.CONNECTED, client.state());

            busyWait("disconnect", () -> client.state() == NetworkClient.State.DISCONNECTED);

            assertEquals(0, callback.getMessages().size());

            callback.assertConnects(1);
            callback.awaitForDisconnects(1);
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

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            expect(IllegalStateException.class, () -> client.connect(server.address(), new NetworkClientCallbackMock<>()));
            expect(IllegalStateException.class, () -> client.connect(server.address(), new NetworkClientCallbackMock<>()));
            expect(IllegalStateException.class, () -> client.connect(server.address(), new NetworkClientCallbackMock<>()));

            assertSame(NetworkClient.State.CONNECTED, client.state());

            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            callback.assertConnects(1);
            callback.assertDisconnects(1);
            callback.assertNoErrors();

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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            client.disconnect();
            client.disconnect();
            client.disconnect().get();

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

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
                assertSame(NetworkClient.State.DISCONNECTED, closeable.state());

                closeable.connect(server.address(), callback).get();

                assertSame(NetworkClient.State.CONNECTED, closeable.state());
            }

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

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

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());

            server.stop().get();

            callback.awaitForDisconnects(1);

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

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

        client.connect(server.address(), callback).get();

        await(disconnectLatch);

        disconnectFuture.get().get();

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        callback.assertConnects(1);
        callback.assertDisconnects(1);
        callback.assertErrors(0);
    }

    @Test
    public void testDisconnectNoWait() throws Exception {
        InetSocketAddress address = newServerAddress();

        try (ServerSocket ignore = new ServerSocket(address.getPort())) {
            for (int i = 20; i >= 0; i--) {
                NetworkClient<String> client = createClient();

                NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

                NetworkFuture<String> connect = client.connect(address, callback);

                NetworkFuture<String> disconnect = client.disconnect();

                get(disconnect);

                assertSame(NetworkClient.State.DISCONNECTED, client.state());

                expectCause(ConnectException.class, () ->
                    get(connect)
                );

                callback.assertErrors(1);
            }
        }
    }

    @Test
    public void testWrongProtocol() throws Exception {
        NetworkServer server = createServer(createHandler("WRONG-PROTOCOL", (msg, from) -> {
            // No-op.
        }));

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

        try {
            get(client.connect(server.address(), callback));

            fail("Error was expected");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectException.class, e));
        }

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        callback.assertConnects(0);
        callback.assertDisconnects(1);
        callback.assertErrors(1);
        callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof ConnectException));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWrongMessageType() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress()).get();

        NetworkClient client = createClient();

        get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

        repeat(3, i -> {
            Object wrongType = new Object();

            NetworkSendCallbackMock callback = new NetworkSendCallbackMock();

            client.send(wrongType, callback);

            callback.awaitForErrors(wrongType);

            Throwable err = callback.getFailure(wrongType);

            assertTrue(err.toString(), err instanceof CodecException);
            assertEquals("Unsupported message type [expected=java.lang.String, real=java.lang.Object]", err.getMessage());

            assertSame(NetworkClient.State.CONNECTED, client.state());
        });
    }

    @Test
    public void testWrongProtocolWithDeferredMessages() throws Exception {
        NetworkServer server = createServer(createHandler("WRONG-PROTOCOL", (msg, from) -> {
            // No-op.
        }));

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        repeat(5, i -> {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            NetworkFuture<String> connect = client.connect(server.address(), callback);

            NetworkSendCallbackMock<String> messageCallback = new NetworkSendCallbackMock<>();

            for (int j = 0; j < i + 1; j++) {
                client.send("msg-" + j + '-' + i, messageCallback);
            }

            try {
                connect.get();

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectException.class, e));
            }

            messageCallback.awaitForSentOrFailed(i + 1);

            messageCallback.assertSent(0);
            messageCallback.assertFailed(i + 1);

            assertSame(NetworkClient.State.DISCONNECTED, client.state());

            callback.assertConnects(0);
            callback.assertDisconnects(1);
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

        client.connect(server.address(), callback).get();

        assertSame(NetworkClient.State.CONNECTED, client.state());

        client.send("test");

        serverListener.awaitForMessages(client.localAddress(), "test");

        client.disconnect().get();

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        server.removeHandler(TEST_PROTOCOL);

        try {
            client.connect(server.address(), callback).get();

            fail("Error was expected");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectException.class, e));
        }

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        callback.assertConnects(1);
        callback.assertDisconnects(2);
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

            InetSocketAddress addr = newNode().socket();

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

                    get(future);

                    fail("Error was expected");
                } catch (ExecutionException e) {
                    assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectTimeoutException.class, e));
                }
            });

            callback.assertConnects(0);
            callback.assertErrors(1);

            messageCallback.awaitForErrors("one", "two", "three");

            callback.getErrors().forEach(e -> assertTrue(e.toString(), e instanceof NetworkConnectTimeoutException));
        } finally {
            stopLatch.countDown();

            if (channel != null) {
                channel.close().await();
            }

            NettyUtils.shutdown(serverGroup).awaitUninterruptedly();

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

            assertSame(NetworkClient.State.DISCONNECTED, badClient.state());
            assertSame(NetworkClient.State.DISCONNECTED, goodClient.state());

            NetworkFuture<String> badFuture = badClient.connect(new InetSocketAddress("localhost", 15000 + i), badCallback);

            NetworkFuture<String> goodFuture = goodClient.connect(server.address(), goodCallback);

            sayTime("Good connect", goodFuture::get);
            sayTime("Bad connect", () -> {
                try {
                    badFuture.get();

                    fail("Error was expected.");
                } catch (ExecutionException e) {
                    assertTrue(e.getCause().toString(), ErrorUtils.isCausedBy(ConnectException.class, e));
                }
            });

            assertSame(NetworkClient.State.CONNECTED, goodClient.state());
            assertSame(NetworkClient.State.DISCONNECTED, badClient.state());

            badCallback.assertConnects(0);
            badCallback.assertDisconnects(1);
            badCallback.assertErrors(1);

            goodClient.disconnect().get();

            goodCallback.assertConnects(1);
            goodCallback.assertDisconnects(1);
            goodCallback.assertNoErrors();

            listener.assertNoErrors();
        });
    }
}
