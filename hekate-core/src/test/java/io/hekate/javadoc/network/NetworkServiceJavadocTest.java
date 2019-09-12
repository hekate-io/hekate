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

package io.hekate.javadoc.network;

import io.hekate.HekateNodeTestBase;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class NetworkServiceJavadocTest extends HekateNodeTestBase {
    // Start:server_handler_example
    // Prepare server handler (String - base type of exchanged messages)
    public static class ExampleHandler implements NetworkServerHandler<String> {
        @Override
        public void onConnect(String loginMsg, NetworkEndpoint<String> client) {
            System.out.println("Got new connection: " + loginMsg);

            // Initialize connection context object.
            client.setContext(new AtomicInteger());
        }

        @Override
        public void onMessage(NetworkMessage<String> netMsg, NetworkEndpoint<String> from) throws IOException {
            String msg = netMsg.decode();

            System.out.println("Message from client: " + msg);

            AtomicInteger counter = (AtomicInteger)from.getContext();

            // Send reply.
            from.send(msg + " processed (total=" + counter.incrementAndGet() + ')');
        }

        @Override
        public void onDisconnect(NetworkEndpoint<String> client) {
            System.out.println("Closed connection.");
        }
    }
    // End:server_handler_example

    @Test
    public void exampleTcpService() throws Exception {
        // Start:config
        // Prepare network service factory.
        NetworkServiceFactory factory = new NetworkServiceFactory()
            // Configure options.
            .withPort(10012) // Cluster node network port.
            .withPortRange(100) // ... if port is busy then auto-increment it
            .withNioThreads(10) // Number of threads to serve NIO operations
            .withConnectTimeout(5000) // Timeout of connecting to a remote peer
            .withHeartbeatInterval(500) // Heartbeat interval for each socket connection to keep alive
            .withHeartbeatLossThreshold(4); // Maximum amount of lost heartbeats
        // ... other options ...

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();

        // Access the service.
        NetworkService network = hekate.network();
        // End:config

        assertNotNull(network);

        // Start:ping
        hekate.network().ping(new InetSocketAddress("127.0.0.1", 10012), (address, result) -> {
            switch (result) {
                case SUCCESS: {
                    System.out.println("Node is alive at " + address);

                    break;
                }
                case FAILURE: {
                    System.out.println("No node at " + address);

                    break;
                }
                case TIMEOUT: {
                    System.out.println("Ping timeout " + address);

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unsupported ping result: " + result);
                }
            }
        });
        // End:ping

        hekate.leave();
    }

    @Test
    public void exampleClientServer() throws Exception {
        // Start:server_handler_config_example
        NetworkConnectorConfig<String> connCfg = new NetworkConnectorConfig<>();

        connCfg.setProtocol("example.protocol"); // Protocol identifier.
        connCfg.setMessageCodec(new JdkCodecFactory<>()); // Use default Java serialization.
        connCfg.setServerHandler(new ExampleHandler()); // Server handler.
        // End:server_handler_config_example

        // Start:server_example
        // Prepare network service factory and register the connector configuration.
        NetworkServiceFactory factory = new NetworkServiceFactory()
            .withPort(10023) // Fixed port for demo purposes.
            .withConnector(connCfg);

        // Register the network service factory and start new node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:server_example

        // Start:client_connect_example
        // Get connector by its protocol identifier.
        NetworkConnector<String> connector = hekate.network().connector("example.protocol");

        NetworkClient<String> client = connector.newClient();

        InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 10023);

        // Asynchronously connect, send a login message (optional) and start receiving messages.
        client.connect(serverAddress, "example login", (message, from) ->
            System.out.println("Message from server: " + message)
        );
        // End:client_connect_example

        // Start:client_send_example
        // Send some messages.
        for (int i = 0; i < 10; i++) {
            client.send("Example message", (msg, err) -> {
                // Check error.
                if (err == null) {
                    System.out.println("Successfully sent: " + msg);
                } else {
                    System.err.println("Failed to send: " + err);
                }
            });
        }

        // Disconnect and await for completion.
        client.disconnect().get();
        // End:client_send_example

        hekate.leave();
    }
}
