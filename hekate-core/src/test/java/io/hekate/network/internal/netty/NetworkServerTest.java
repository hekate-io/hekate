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
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.internal.util.Utils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.internal.NetworkClientCallbackMock;
import io.hekate.network.internal.NetworkSendCallbackMock;
import io.hekate.network.internal.NetworkServer;
import io.hekate.network.internal.NetworkServerCallbackMock;
import io.hekate.network.internal.NetworkServerFailure;
import io.hekate.network.internal.NetworkServerHandlerConfig;
import io.hekate.network.internal.NetworkTestBase;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkServerTest extends NetworkTestBase {
    public static class TestIoException extends IOException {
        private static final long serialVersionUID = 1;

        public TestIoException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    public NetworkServerTest(HekateTestContext textContext) {
        super(textContext);
    }

    @Test
    public void testStartStop() throws Exception {
        NetworkServer server = createServer();

        repeat(5, i -> {
            NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

            say("Starting...");

            assertSame(NetworkServer.State.STOPPED, server.getState());

            server.start(newServerAddress(), listener).get();

            assertSame(NetworkServer.State.STARTED, server.getState());

            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertNoErrors();

            say("Stopping...");

            server.stop().get();

            assertSame(NetworkServer.State.STOPPED, server.getState());

            listener.assertStarts(1);
            listener.assertStops(1);
            listener.assertNoErrors();

            say("Done.");
        });
    }

    @Test
    public void testPortAssignment() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        say("Starting...");

        assertSame(NetworkServer.State.STOPPED, server.getState());

        server.start(new InetSocketAddress(0), listener).get();

        assertSame(NetworkServer.State.STARTED, server.getState());
        assertTrue(server.getAddress().getPort() > 0);

        say("Started on port " + server.getAddress().getPort());

        listener.assertStarts(1);
        listener.assertStops(0);
        listener.assertNoErrors();

        say("Stopping...");

        server.stop().get();

        assertSame(NetworkServer.State.STOPPED, server.getState());

        // Port must be preserved after stop.
        assertTrue(server.getAddress().getPort() > 0);

        listener.assertStarts(1);
        listener.assertStops(1);
        listener.assertNoErrors();

        say("Done.");
    }

    @Test
    public void testFailover() throws Exception {
        int failoverInterval = 50;

        NettyServer nettyServer = (NettyServer)createServer();

        AtomicReference<CountDownLatch> errorLatch = new AtomicReference<>();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock() {
            @Override
            public NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure failure) {
                super.onFailure(server, failure);

                errorLatch.get().countDown();

                return failure.retry().withRetryDelay(failoverInterval);
            }
        };

        nettyServer.start(newServerAddress(), listener).get();

        repeat(3, i -> {
            errorLatch.set(new CountDownLatch(1));

            assertSame(NetworkServer.State.STARTED, nettyServer.getState());

            listener.assertStarts(1);
            listener.assertStops(0);

            nettyServer.getServerChannel().pipeline().fireExceptionCaught(new TestIoException(TEST_ERROR_MESSAGE));

            await(errorLatch.get());

            listener.assertErrors(i + 1);

            sleep(failoverInterval * 2);

            assertSame(NetworkServer.State.STARTED, nettyServer.getState());
            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertErrors(i + 1);

            listener.getErrors().forEach(e -> {
                assertTrue(e.toString(), e instanceof IOException);
                assertEquals(TEST_ERROR_MESSAGE, e.getMessage());
            });
        });
    }

    @Test
    public void testAddressChangeOnFailover() throws Exception {
        int failoverInterval = 50;

        NettyServer nettyServer = (NettyServer)createServer();

        AtomicReference<CountDownLatch> errorLatch = new AtomicReference<>();

        InetSocketAddress initAddress = newServerAddress();

        assertTrue(initAddress.getPort() > 0);

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock() {
            @Override
            public NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure failure) {
                super.onFailure(server, failure);

                errorLatch.get().countDown();

                InetSocketAddress newAddress = new InetSocketAddress(initAddress.getAddress(), failure.getLastTriedAddress().getPort() + 1);

                return failure.retry().withRetryDelay(failoverInterval).withRetryAddress(newAddress);
            }
        };

        nettyServer.start(initAddress, listener).get();

        repeat(3, i -> {
            errorLatch.set(new CountDownLatch(1));

            assertSame(NetworkServer.State.STARTED, nettyServer.getState());

            listener.assertStarts(1);
            listener.assertStops(0);

            nettyServer.getServerChannel().pipeline().fireExceptionCaught(new TestIoException(TEST_ERROR_MESSAGE));

            await(errorLatch.get());

            listener.assertErrors(i + 1);

            sleep(failoverInterval * 2);

            assertSame(NetworkServer.State.STARTED, nettyServer.getState());
            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertErrors(i + 1);

            assertEquals(initAddress.getPort() + i + 1, nettyServer.getAddress().getPort());

            listener.getErrors().forEach(e -> {
                assertTrue(e.toString(), e instanceof IOException);
                assertEquals(TEST_ERROR_MESSAGE, e.getMessage());
            });
        });
    }

    @Test
    public void testMultipleStop() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock() {
            @Override
            public void onStop(NetworkServer stopped) {
                super.onStop(stopped);

                sleep(300);
            }
        };

        server.start(newServerAddress(), listener).get();

        server.stop();
        server.stop();
        server.stop().get();

        listener.assertNoErrors();
        listener.assertStarts(1);
        listener.assertStops(1);
    }

    @Test
    public void testMultipleServers() throws Exception {
        List<NetworkServer> servers = new ArrayList<>();

        say("Prepare.");

        repeat(5, i -> servers.add(createServer()));

        say("Run.");

        repeat(5, i -> startAndStop(servers));
    }

    @Test
    public void testAutoAccept() throws Exception {
        NetworkServer server = createAndConfigureServer(f -> f.setAutoAccept(false));

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        try {
            client.connect(server.getAddress(), new NetworkClientCallbackMock<>()).get();

            fail();
        } catch (ExecutionException e) {
            assertTrue(Utils.isCausedBy(e, ConnectException.class));
        }

        server.startAccepting();

        client.connect(server.getAddress(), new NetworkClientCallbackMock<>()).get();
    }

    @Test
    public void testUnregisterHandlerWithClients() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        List<NetworkClientCallbackMock<String>> callbacks = new LinkedList<>();

        for (int i = 0; i < 3; i++) {
            NetworkClient<String> client = createClient();

            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            callbacks.add(callback);

            client.connect(server.getAddress(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.getState());
        }

        List<NetworkEndpoint<?>> clients = server.removeHandler(TEST_PROTOCOL);

        assertEquals(callbacks.size(), clients.size());

        clients.forEach(NetworkEndpoint::disconnect);

        for (NetworkClientCallbackMock<String> callback : callbacks) {
            callback.awaitForDisconnects(1);
        }
    }

    @Test
    public void testDuplicatedProtocolFailure() throws Exception {
        NetworkServer server = createServer();

        try {
            server.addHandler(createHandler((message, from) -> {
                // No-op.
            }));

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(e.toString(), e.getMessage(), NetworkServerHandlerConfig.class.getSimpleName()
                + ": duplicated protocol [value=test]");
        }

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        try {
            server.addHandler(createHandler((message, from) -> {
                // No-op.
            }));

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(e.toString(), e.getMessage(), NetworkServerHandlerConfig.class.getSimpleName()
                + ": duplicated protocol [value=test]");
        }
    }

    @Test
    public void testHandlerContext() throws Exception {
        int clients = 5;
        int messagesPerClient = 5;

        CountDownLatch ready = new CountDownLatch(clients);

        Map<String, List<String>> contexts = new ConcurrentHashMap<>();

        NetworkServer server = createServer();

        server.addHandler(createHandler("test_context", new NetworkServerHandler<String>() {
            @Override
            public void onConnect(String msg, NetworkEndpoint<String> client) {
                List<String> events = new LinkedList<>();

                contexts.put(msg, events);

                events.add(msg);

                client.setContext(events);
            }

            @Override
            public void onDisconnect(NetworkEndpoint<String> client) {
                List<String> events = client.getContext();

                events.add("disconnect");

                ready.countDown();
            }

            @Override
            public void onFailure(NetworkEndpoint<String> client, Throwable cause) {
                List<String> events = client.getContext();

                events.add(cause.getMessage());
            }

            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) throws IOException {
                if (msg.decode().equals("fail")) {
                    throw TEST_ERROR;
                } else {
                    List<String> events = from.getContext();

                    events.add(msg.decode());
                }
            }
        }));

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        for (int i = 0; i < clients; i++) {
            NetworkClient<String> client = createClient(c -> c.setProtocol("test_context"));

            client.connect(server.getAddress(), "client_" + i, new NetworkClientCallbackMock<>()).get();

            for (int j = 1; j <= messagesPerClient; j++) {
                client.send("msg_" + i + "_" + j);
            }

            client.send("fail");

            client.disconnect().get();
        }

        await(ready);

        server.stop().get();

        for (int i = 0; i < clients; i++) {
            List<String> events = contexts.get("client_" + i);

            assertNotNull(events);
            assertEquals("client_" + i, events.get(0));

            for (int j = 1; j <= messagesPerClient; j++) {
                assertEquals("msg_" + i + "_" + j, events.get(j));
            }

            assertEquals(TEST_ERROR_MESSAGE, events.get(messagesPerClient + 1));
            assertEquals("disconnect", events.get(messagesPerClient + 2));
        }
    }

    @Test
    public void testServerClientInExternalThread() throws Exception {
        NetworkServer server = createServer();

        CompletableFuture<NetworkEndpoint<String>> latch = new CompletableFuture<>();

        server.addHandler(createHandler("external_client", new NetworkServerHandler<String>() {
            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) {
                // No-op.
            }

            @Override
            public void onConnect(String msg, NetworkEndpoint<String> client) {
                latch.complete(client);
            }
        }));

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient(c -> c.setProtocol("external_client"));

        NetworkClientCallbackMock<String> clientCallback = new NetworkClientCallbackMock<>();

        client.connect(server.getAddress(), clientCallback);

        NetworkEndpoint<String> serverClient = get(latch);

        repeat(5, i -> {
            serverClient.send("test" + i);

            clientCallback.awaitForMessages("test" + i);
        });

        server.stop().get();

        NetworkSendCallbackMock<String> failCallback = new NetworkSendCallbackMock<>();

        serverClient.send("fail", failCallback);

        failCallback.awaitForErrors("fail");

        assertTrue(failCallback.getFailure("fail") instanceof ClosedChannelException);
    }

    @Test
    public void testGetConnected() throws Exception {
        // Prepare server.
        NetworkServer server = createServer();

        server.addHandler(createHandler("protocol_1", (msg, from) -> {
            // No-op.
        }));
        server.addHandler(createHandler("protocol_2", (msg, from) -> {
            // No-op.
        }));

        assertTrue(server.getConnected("protocol_1").isEmpty());
        assertTrue(server.getConnected("protocol_2").isEmpty());
        assertTrue(server.getConnected("some_unknown_protocol").isEmpty());

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        repeat(3, i -> {
            // Connect clients.
            NetworkClient<String> p1c1 = createClient(c -> c.setProtocol("protocol_1"));
            NetworkClient<String> p1c2 = createClient(c -> c.setProtocol("protocol_1"));
            NetworkClient<String> p2c1 = createClient(c -> c.setProtocol("protocol_2"));
            NetworkClient<String> p2c2 = createClient(c -> c.setProtocol("protocol_2"));

            get(p1c1.connect(server.getAddress(), new NetworkClientCallbackMock<>()));
            get(p1c2.connect(server.getAddress(), new NetworkClientCallbackMock<>()));
            get(p2c1.connect(server.getAddress(), new NetworkClientCallbackMock<>()));
            get(p2c2.connect(server.getAddress(), new NetworkClientCallbackMock<>()));

            // Verify that for each client there is a server endpoint.
            List<NetworkEndpoint<?>> connectedP1 = server.getConnected("protocol_1");
            List<NetworkEndpoint<?>> connectedP2 = server.getConnected("protocol_2");

            assertEquals(2, connectedP1.size());
            assertTrue(connectedP1.stream().anyMatch(e -> e.getRemoteAddress().equals(p1c1.getLocalAddress())));
            assertTrue(connectedP1.stream().anyMatch(e -> e.getRemoteAddress().equals(p1c2.getLocalAddress())));

            assertEquals(2, connectedP2.size());
            assertTrue(connectedP2.stream().anyMatch(e -> e.getRemoteAddress().equals(p2c1.getLocalAddress())));
            assertTrue(connectedP2.stream().anyMatch(e -> e.getRemoteAddress().equals(p2c2.getLocalAddress())));

            // Disconnect 1 client from each protocol.
            get(p1c1.disconnect());
            get(p2c1.disconnect());

            busyWait("'protocol_1' server disconnect", () -> server.getConnected("protocol_1").size() == 1);
            busyWait("'protocol_1' server disconnect", () -> server.getConnected("protocol_2").size() == 1);

            // Verify that clients were removed from server endpoints list.
            connectedP1 = server.getConnected("protocol_1");
            connectedP2 = server.getConnected("protocol_2");

            assertEquals(1, connectedP1.size());
            assertTrue(connectedP1.stream().anyMatch(e -> e.getRemoteAddress().equals(p1c2.getLocalAddress())));

            assertEquals(1, connectedP2.size());
            assertTrue(connectedP2.stream().anyMatch(e -> e.getRemoteAddress().equals(p2c2.getLocalAddress())));

            // Disconnect server endpoints.
            get(connectedP1.get(0).disconnect());
            get(connectedP2.get(0).disconnect());

            busyWait("'protocol_1' client disconnect", () -> p1c2.getState() == NetworkClient.State.DISCONNECTED);
            busyWait("'protocol_2' client disconnect", () -> p2c2.getState() == NetworkClient.State.DISCONNECTED);

            // Verify that clients were disconnected too.
            assertSame(NetworkClient.State.DISCONNECTED, p1c1.getState());
            assertSame(NetworkClient.State.DISCONNECTED, p1c2.getState());
            assertSame(NetworkClient.State.DISCONNECTED, p2c1.getState());
            assertSame(NetworkClient.State.DISCONNECTED, p2c2.getState());
        });
    }

    @Test
    public void testInvalidMagicBytes() throws Exception {
        InetSocketAddress addr = newServerAddress();

        NetworkServer server = createServer();

        server.start(addr, new NetworkServerCallbackMock()).get();

        repeat(3, i -> {
            try (
                Socket socket = new Socket(addr.getAddress(), addr.getPort());
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream()
            ) {
                // Invalid magic bytes.
                out.write(new byte[]{1, 2, 3, 4, 5, 6, 7});
                out.flush();

                assertEquals(-1, in.read());
            }
        });
    }

    @Test
    public void testInvalidAddress() throws Exception {
        // Prepare server.
        NetworkServer server = createServer();

        try {
            InetSocketAddress address = new InetSocketAddress("254.254.254.254", 0);

            get(server.start(address));

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e), Utils.isCausedBy(e, IOException.class));
        }
    }

    private void startAndStop(List<NetworkServer> servers) throws Exception {
        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        for (NetworkServer server : servers) {
            server.start(newServerAddress(), listener).get();
        }

        for (NetworkServer server : servers) {
            assertSame(NetworkServer.State.STARTED, server.getState());

            server.stop().get();

            assertSame(NetworkServer.State.STOPPED, server.getState());
        }

        listener.assertStarts(servers.size());
        listener.assertStops(servers.size());
        listener.assertNoErrors();
    }
}
