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
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkSendCallbackMock;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerCallbackMock;
import io.hekate.network.NetworkServerFailure;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkServerHandlerConfig;
import io.hekate.network.internal.NetworkTestBase;
import io.hekate.test.HekateTestError;
import io.hekate.test.NetworkClientCallbackMock;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assume;
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

            assertSame(NetworkServer.State.STOPPED, server.state());

            server.start(newServerAddress(), listener).get();

            assertSame(NetworkServer.State.STARTED, server.state());

            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertNoErrors();

            say("Stopping...");

            server.stop().get();

            assertSame(NetworkServer.State.STOPPED, server.state());

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

        assertSame(NetworkServer.State.STOPPED, server.state());

        server.start(new InetSocketAddress(0), listener).get();

        assertSame(NetworkServer.State.STARTED, server.state());
        assertTrue(server.address().getPort() > 0);

        say("Started on port " + server.address().getPort());

        listener.assertStarts(1);
        listener.assertStops(0);
        listener.assertNoErrors();

        say("Stopping...");

        server.stop().get();

        assertSame(NetworkServer.State.STOPPED, server.state());

        // Port must be preserved after stop.
        assertTrue(server.address().getPort() > 0);

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

            assertSame(NetworkServer.State.STARTED, nettyServer.state());

            listener.assertStarts(1);
            listener.assertStops(0);

            nettyServer.nettyChannel().get().pipeline().fireExceptionCaught(new TestIoException(HekateTestError.MESSAGE));

            await(errorLatch.get());

            listener.assertErrors(i + 1);

            sleep(failoverInterval * 2);

            assertSame(NetworkServer.State.STARTED, nettyServer.state());
            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertErrors(i + 1);

            listener.getErrors().forEach(e -> {
                assertTrue(e.toString(), e instanceof IOException);
                assertEquals(HekateTestError.MESSAGE, e.getMessage());
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

                InetSocketAddress newAddress = new InetSocketAddress(initAddress.getAddress(), failure.lastTriedAddress().getPort() + 1);

                return failure.retry().withRetryDelay(failoverInterval).withRetryAddress(newAddress);
            }
        };

        nettyServer.start(initAddress, listener).get();

        repeat(3, i -> {
            errorLatch.set(new CountDownLatch(1));

            assertSame(NetworkServer.State.STARTED, nettyServer.state());

            listener.assertStarts(1);
            listener.assertStops(0);

            nettyServer.nettyChannel().get().pipeline().fireExceptionCaught(new TestIoException(HekateTestError.MESSAGE));

            await(errorLatch.get());

            listener.assertErrors(i + 1);

            sleep(failoverInterval * 2);

            assertSame(NetworkServer.State.STARTED, nettyServer.state());
            listener.assertStarts(1);
            listener.assertStops(0);
            listener.assertErrors(i + 1);

            assertEquals(initAddress.getPort() + i + 1, nettyServer.address().getPort());

            listener.getErrors().forEach(e -> {
                assertTrue(e.toString(), e instanceof IOException);
                assertEquals(HekateTestError.MESSAGE, e.getMessage());
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
            client.connect(server.address(), new NetworkClientCallbackMock<>()).get();

            fail();
        } catch (ExecutionException e) {
            assertTrue(ErrorUtils.isCausedBy(ConnectException.class, e));
        }

        server.startAccepting();

        client.connect(server.address(), new NetworkClientCallbackMock<>()).get();
    }

    @Test
    public void testUnregisterHandlerWithClients() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        List<NetworkClientCallbackMock<String>> callbacks = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            NetworkClient<String> client = createClient();

            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            callbacks.add(callback);

            client.connect(server.address(), callback).get();

            assertSame(NetworkClient.State.CONNECTED, client.state());
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

        CountDownLatch clientsDisconnected = new CountDownLatch(clients);

        Map<String, List<String>> contexts = new ConcurrentHashMap<>();

        NetworkServer server = createServer();

        server.addHandler(createHandler("test-context", new NetworkServerHandler<String>() {
            @Override
            public void onConnect(String msg, NetworkEndpoint<String> client) {
                List<String> events = new ArrayList<>();

                contexts.put(msg, events);

                events.add(msg);

                client.setContext(events);
            }

            @Override
            public void onDisconnect(NetworkEndpoint<String> client) {
                @SuppressWarnings("unchecked")
                List<String> events = (List<String>)client.getContext();

                events.add("disconnect");

                clientsDisconnected.countDown();
            }

            @Override
            public void onFailure(NetworkEndpoint<String> client, Throwable cause) {
                @SuppressWarnings("unchecked")
                List<String> events = (List<String>)client.getContext();

                events.add(cause.getMessage());
            }

            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) throws IOException {
                if (msg.decode().equals("fail")) {
                    throw TEST_ERROR;
                } else {
                    @SuppressWarnings("unchecked")
                    List<String> events = (List<String>)from.getContext();

                    events.add(msg.decode());
                }
            }
        }));

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        for (int i = 0; i < clients; i++) {
            NetworkClient<String> client = createClient(c -> c.setProtocol("test-context"));

            client.connect(server.address(), "client_" + i, new NetworkClientCallbackMock<>()).get();

            for (int j = 1; j <= messagesPerClient; j++) {
                client.send("msg_" + i + "_" + j);
            }

            CompletableFuture<String> failSend = new CompletableFuture<>();

            client.send("fail", (msg, err) -> {
                if (err == null) {
                    failSend.complete(msg);
                } else {
                    failSend.completeExceptionally(err);
                }
            });

            failSend.thenRun(client::disconnect);
        }

        await(clientsDisconnected);

        server.stop().get();

        for (int i = 0; i < clients; i++) {
            List<String> events = contexts.get("client_" + i);

            assertNotNull(events);
            assertEquals("client_" + i, events.get(0));

            for (int j = 1; j <= messagesPerClient; j++) {
                assertEquals("msg_" + i + "_" + j, events.get(j));
            }

            assertEquals(HekateTestError.MESSAGE, events.get(messagesPerClient + 1));
            assertEquals("disconnect", events.get(messagesPerClient + 2));
        }
    }

    @Test
    public void testServerClientInExternalThread() throws Exception {
        NetworkServer server = createServer();

        CompletableFuture<NetworkEndpoint<String>> serverClientFuture = new CompletableFuture<>();

        server.addHandler(createHandler("external-client", new NetworkServerHandler<String>() {
            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) {
                // No-op.
            }

            @Override
            public void onConnect(String msg, NetworkEndpoint<String> client) {
                serverClientFuture.complete(client);
            }
        }));

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient(c -> c.setProtocol("external-client"));

        NetworkClientCallbackMock<String> clientCallback = new NetworkClientCallbackMock<>();

        client.connect(server.address(), clientCallback);

        NetworkEndpoint<String> serverClient = get(serverClientFuture);

        assertEquals(context().ssl().isPresent(), serverClient.isSecure());

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
    @SuppressWarnings("unchecked")
    public void testWrongMessageType() throws Exception {
        NetworkServer server = createServer();

        CompletableFuture<NetworkEndpoint<String>> serverClientFuture = new CompletableFuture<>();

        server.addHandler(createHandler("external-client", new NetworkServerHandler<String>() {
            @Override
            public void onMessage(NetworkMessage<String> msg, NetworkEndpoint<String> from) {
                // No-op.
            }

            @Override
            public void onConnect(String msg, NetworkEndpoint<String> client) {
                serverClientFuture.complete(client);
            }
        }));

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient(c -> c.setProtocol("external-client"));

        client.connect(server.address(), new NetworkClientCallbackMock<>());

        NetworkEndpoint serverClient = get(serverClientFuture);

        repeat(3, i -> {
            Object wrongType = new Object();

            NetworkSendCallbackMock callback = new NetworkSendCallbackMock();

            serverClient.send(wrongType, callback);

            callback.awaitForErrors(wrongType);

            Throwable err = callback.getFailure(wrongType);

            assertTrue(err.toString(), err instanceof CodecException);
            assertEquals("Unsupported message type [expected=java.lang.String, real=java.lang.Object]", err.getMessage());
        });
    }

    @Test
    public void testGetConnected() throws Exception {
        // Prepare server.
        NetworkServer server = createServer();

        server.addHandler(createHandler("protocol-1", (msg, from) -> {
            // No-op.
        }));
        server.addHandler(createHandler("protocol-2", (msg, from) -> {
            // No-op.
        }));

        assertTrue(server.clients("protocol-1").isEmpty());
        assertTrue(server.clients("protocol-2").isEmpty());
        assertTrue(server.clients("some-unknown-protocol").isEmpty());

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        repeat(3, i -> {
            // Connect clients.
            NetworkClient<String> p1c1 = createClient(c -> c.setProtocol("protocol-1"));
            NetworkClient<String> p1c2 = createClient(c -> c.setProtocol("protocol-1"));
            NetworkClient<String> p2c1 = createClient(c -> c.setProtocol("protocol-2"));
            NetworkClient<String> p2c2 = createClient(c -> c.setProtocol("protocol-2"));

            get(p1c1.connect(server.address(), new NetworkClientCallbackMock<>()));
            get(p1c2.connect(server.address(), new NetworkClientCallbackMock<>()));
            get(p2c1.connect(server.address(), new NetworkClientCallbackMock<>()));
            get(p2c2.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Verify that for each client there is a server endpoint.
            List<NetworkEndpoint<?>> connectedP1 = server.clients("protocol-1");
            List<NetworkEndpoint<?>> connectedP2 = server.clients("protocol-2");

            assertEquals(2, connectedP1.size());
            assertTrue(connectedP1.stream().anyMatch(e -> e.remoteAddress().equals(p1c1.localAddress())));
            assertTrue(connectedP1.stream().anyMatch(e -> e.remoteAddress().equals(p1c2.localAddress())));

            assertEquals(2, connectedP2.size());
            assertTrue(connectedP2.stream().anyMatch(e -> e.remoteAddress().equals(p2c1.localAddress())));
            assertTrue(connectedP2.stream().anyMatch(e -> e.remoteAddress().equals(p2c2.localAddress())));

            // Disconnect 1 client from each protocol.
            get(p1c1.disconnect());
            get(p2c1.disconnect());

            busyWait("'protocol-1' server disconnect", () -> server.clients("protocol-1").size() == 1);
            busyWait("'protocol-1' server disconnect", () -> server.clients("protocol-2").size() == 1);

            // Verify that clients were removed from server endpoints list.
            connectedP1 = server.clients("protocol-1");
            connectedP2 = server.clients("protocol-2");

            assertEquals(1, connectedP1.size());
            assertTrue(connectedP1.stream().anyMatch(e -> e.remoteAddress().equals(p1c2.localAddress())));

            assertEquals(1, connectedP2.size());
            assertTrue(connectedP2.stream().anyMatch(e -> e.remoteAddress().equals(p2c2.localAddress())));

            // Disconnect server endpoints.
            get(connectedP1.get(0).disconnect());
            get(connectedP2.get(0).disconnect());

            busyWait("'protocol-1' client disconnect", () -> p1c2.state() == NetworkClient.State.DISCONNECTED);
            busyWait("'protocol-2' client disconnect", () -> p2c2.state() == NetworkClient.State.DISCONNECTED);

            // Verify that clients were disconnected too.
            assertSame(NetworkClient.State.DISCONNECTED, p1c1.state());
            assertSame(NetworkClient.State.DISCONNECTED, p1c2.state());
            assertSame(NetworkClient.State.DISCONNECTED, p2c1.state());
            assertSame(NetworkClient.State.DISCONNECTED, p2c2.state());

            busyWait("'protocol-1' all clients disconnect", () -> server.clients("protocol-1").isEmpty());
            busyWait("'protocol-2' all clients disconnect", () -> server.clients("protocol-2").isEmpty());
        });
    }

    @Test
    public void testInvalidMagicBytes() throws Exception {
        // This test sends fake bytes upon connection and can't work properly in SSL context.
        Assume.assumeFalse(context().ssl().isPresent());

        InetSocketAddress addr = newServerAddress();

        NetworkServer server = createAndConfigureServer(cfg ->
            cfg.setDisableHeartbeats(true)
        );

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
    public void testInvalidProtocolVersion() throws Exception {
        // This test sends fake bytes upon connection and can't work properly in SSL context.
        Assume.assumeFalse(context().ssl().isPresent());

        InetSocketAddress addr = newServerAddress();

        NetworkServer server = createAndConfigureServer(cfg ->
            cfg.setDisableHeartbeats(true)
        );

        server.start(addr, new NetworkServerCallbackMock()).get();

        repeat(3, i -> {
            try (
                Socket socket = new Socket(addr.getAddress(), addr.getPort());
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream()
            ) {
                // Valid magic bytes
                out.write((byte)(Utils.MAGIC_BYTES >> 24));
                out.write((byte)(Utils.MAGIC_BYTES >> 16));
                out.write((byte)(Utils.MAGIC_BYTES >> 8));
                out.write((byte)Utils.MAGIC_BYTES);

                // Invalid version.
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
            assertTrue(getStacktrace(e), ErrorUtils.isCausedBy(IOException.class, e));
        }
    }

    private void startAndStop(List<NetworkServer> servers) throws Exception {
        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        for (NetworkServer server : servers) {
            server.start(newServerAddress(), listener).get();
        }

        for (NetworkServer server : servers) {
            assertSame(NetworkServer.State.STARTED, server.state());

            server.stop().get();

            assertSame(NetworkServer.State.STOPPED, server.state());
        }

        listener.assertStarts(servers.size());
        listener.assertStops(servers.size());
        listener.assertNoErrors();
    }
}
