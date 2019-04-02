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
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkSendCallbackMock;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerCallbackMock;
import io.hekate.network.NetworkServerHandlerMock;
import io.hekate.network.internal.NetworkTestBase;
import io.hekate.test.NetworkClientCallbackMock;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PauseResumeReceivingTest extends NetworkTestBase {
    public PauseResumeReceivingTest(HekateTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testPauseServerTest() throws Exception {
        NetworkServerHandlerMock<String> receiver = new NetworkServerHandlerMock<>();

        NetworkServer server = createServer(receiver);

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            repeat(3, j -> {
                String msg = "request_" + i;

                // Pause receiving.
                pause(remote);

                // Send a message.
                NetworkSendCallbackMock<String> sendCallback = new NetworkSendCallbackMock<>();

                client.send(msg, sendCallback);

                // Make sure that message was flushed to the network buffer.
                sendCallback.awaitForSent(msg);

                // Await for some time before checking that message wasn't received.
                sleep(50);

                // Check that message was not received.
                receiver.assertNotReceived(client, msg);

                // Resume receiving.
                resume(remote);

                // Check that message was received.
                receiver.awaitForMessages(client, msg);

                receiver.reset();
            });

            get(client.disconnect());
            get(remote.disconnect());

            // Check that pause/resume after disconnect doesn't cause errors.
            doPause(remote);
            doResume(remote);
        });
    }

    @Test
    public void testClientDisconnectWhileServerPausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            pause(remote);

            get(client.disconnect());

            // Await for server endpoint to be disconnected.
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testClientDisconnectWhileBothPausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            pause(client);
            pause(remote);

            get(client.disconnect());

            // Await for server endpoint to be disconnected.
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testServerDisconnectWhilePausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            pause(remote);

            get(remote.disconnect());

            // Await for client to be disconnected.
            busyWait("client disconnect", () -> client.state() == NetworkClient.State.DISCONNECTED);
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testServerDisconnectWhileClientPausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            pause(client);

            get(remote.disconnect());

            // Await for client to be disconnected.
            busyWait("client disconnect", () -> client.state() == NetworkClient.State.DISCONNECTED);
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testServerDisconnectWhileBothPausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            NetworkEndpoint<?> remote = server.clients(client.protocol()).get(0);

            pause(remote);
            pause(client);

            get(remote.disconnect());

            // Await for client to be disconnected.
            busyWait("client disconnect", () -> client.state() == NetworkClient.State.DISCONNECTED);
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testClientDisconnectWhilePausedTest() throws Exception {
        NetworkServer server = createServer();

        server.start(newServerAddress(), new NetworkServerCallbackMock()).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            get(client.connect(server.address(), new NetworkClientCallbackMock<>()));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint connect", () -> !server.clients(client.protocol()).isEmpty());

            pause(client);

            get(client.disconnect());

            // Await for server endpoint to be disconnected.
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testPauseClientTest() throws Exception {
        NetworkServer server = createServer();

        NetworkServerCallbackMock listener = new NetworkServerCallbackMock();

        server.start(newServerAddress(), listener).get();

        NetworkClient<String> client = createClient();

        repeat(3, i -> {
            NetworkClientCallbackMock<String> clientCallback = new NetworkClientCallbackMock<>();

            get(client.connect(server.address(), clientCallback));

            // Await for server endpoint to be initialized.
            busyWait("server endpoint", () -> !server.clients(client.protocol()).isEmpty());

            @SuppressWarnings("unchecked")
            NetworkEndpoint<String> remote = (NetworkEndpoint<String>)server.clients(client.protocol()).get(0);

            repeat(3, j -> {
                String msg = "request_" + i;

                // Pause receiving.
                pause(client);

                // Send a message.
                NetworkSendCallbackMock<String> sendCallback = new NetworkSendCallbackMock<>();

                remote.send(msg, sendCallback);

                // Make sure that message was flushed to the network buffer.
                sendCallback.awaitForSent(msg);

                // Await for some time before checking that message wasn't received.
                sleep(50);

                // Check that message was not received.
                assertTrue(clientCallback.getMessages().isEmpty());

                // Resume receiving.
                resume(client);

                // Check that message was received.
                clientCallback.awaitForMessages(msg);

                clientCallback.reset();
            });

            get(client.disconnect());

            // Check that pause/resume after disconnect doesn't cause errors.
            doPause(client);
            doResume(client);

            // Await for server endpoint to be disconnected.
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        });
    }

    @Test
    public void testServerNoHeartbeatTimeoutOnPause() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        NetworkServer server = createAndConfigureServer(f -> {
            f.setHeartbeatInterval(hbInterval);
            f.setHeartbeatLossThreshold(hbLossThreshold);
        });

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        CountDownLatch blockLatch = new CountDownLatch(1);

        try {
            NetworkClientCallbackMock<String> clientCallback = new NetworkClientCallbackMock<String>() {
                @Override
                public void onMessage(NetworkMessage<String> msg, NetworkClient<String> from) throws IOException {
                    super.onMessage(msg, client);

                    if (msg.decode().equals("block")) {
                        try {
                            blockLatch.await();
                        } catch (InterruptedException t) {
                            // No-op.
                        }
                    }
                }
            };

            client.connect(server.address(), clientCallback).get();

            busyWait("server endpoint", () -> !server.clients(client.protocol()).isEmpty());

            @SuppressWarnings("unchecked")
            NetworkEndpoint<String> remote = (NetworkEndpoint<String>)server.clients(client.protocol()).get(0);

            repeat(3, i -> {
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                pause(remote);

                sleep(hbInterval * hbLossThreshold * 2);

                assertSame(NetworkClient.State.CONNECTED, client.state());
                assertEquals(0, clientCallback.getMessages().size());
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                resume(remote);

                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                clientCallback.reset();
            });

            remote.send("block");

            // Server endpoint must be disconnected by timeout.
            busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
        } finally {
            blockLatch.countDown();
        }
    }

    @Test
    public void testClientNoHeartbeatTimeoutOnPause() throws Exception {
        int hbInterval = context().hbInterval();
        int hbLossThreshold = context().hbLossThreshold();

        CountDownLatch blockLatch = new CountDownLatch(1);

        NetworkServer server = createAndConfigureServerHandler(
            h ->
                h.setHandler((msg, from) -> {
                    if (msg.decode().equals("block")) {
                        try {
                            blockLatch.await();
                        } catch (InterruptedException e) {
                            // No-op.
                        }
                    }
                }),
            s -> {
                s.setHeartbeatInterval(hbInterval);
                s.setHeartbeatLossThreshold(hbLossThreshold);
            }
        );

        server.start(newServerAddress()).get();

        NetworkClient<String> client = createClient();

        try {
            NetworkClientCallbackMock<String> clientCallback = new NetworkClientCallbackMock<>();

            client.connect(server.address(), clientCallback).get();

            repeat(3, i -> {
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                busyWait("server endpoint", () -> !server.clients(client.protocol()).isEmpty());

                @SuppressWarnings("unchecked")
                NetworkEndpoint<String> remote = (NetworkEndpoint<String>)server.clients(client.protocol()).get(0);

                pause(client);

                sleep(hbInterval * hbLossThreshold * 2);

                assertSame(NetworkClient.State.CONNECTED, client.state());
                assertEquals(0, clientCallback.getMessages().size());
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                resume(client);

                assertSame(NetworkClient.State.CONNECTED, client.state());
                assertEquals(0, clientCallback.getMessages().size());
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                remote.send("ping_" + i);

                clientCallback.awaitForMessages("ping_" + i);
                clientCallback.assertDisconnects(0);
                clientCallback.assertNoErrors();

                clientCallback.reset();
            });

            // Check that timeout is still possible.
            client.send("block");

            // Client must be disconnected by timeout.
            busyWait("disconnect", () -> client.state() == NetworkClient.State.DISCONNECTED);
        } finally {
            blockLatch.countDown();
        }

        busyWait("server endpoint disconnect", () -> server.clients(client.protocol()).isEmpty());
    }

    private void pause(NetworkEndpoint<?> client) {
        assertTrue(client.isReceiving());

        doPause(client);

        assertFalse(client.isReceiving());
    }

    private void resume(NetworkEndpoint<?> client) {
        assertFalse(client.isReceiving());

        doResume(client);

        assertTrue(client.isReceiving());
    }

    private void doPause(NetworkEndpoint<?> client) {
        CountDownLatch paused = new CountDownLatch(1);

        client.pauseReceiving(subj -> paused.countDown());

        await(paused);
    }

    private void doResume(NetworkEndpoint<?> client) {
        CountDownLatch resumed = new CountDownLatch(1);

        client.resumeReceiving(subj -> resumed.countDown());

        await(resumed);
    }
}
