/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.network.internal;

import io.hekate.HekateTestContext;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.core.service.NetworkBindCallback;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.test.NetworkClientCallbackMock;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NettyNetworkServiceTest extends NetworkTestBase {
    private interface Configurer {
        void configure(NetworkServiceFactory cfg);
    }

    private final List<NettyNetworkService> services = new CopyOnWriteArrayList<>();

    public NettyNetworkServiceTest(HekateTestContext textContext) {
        super(textContext);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        services.forEach(NettyNetworkService::stop);

        services.clear();

        super.tearDown();
    }

    @Test
    public void testStartBindStop() throws Exception {
        NettyNetworkService net = createService();

        repeat(5, i -> {
            net.bind(new NetworkBindCallback() {
                // No-op.
            }).get();

            net.start();

            net.stop();
        });
    }

    @Test
    public void testClientAfterServiceStop() throws Exception {
        NettyNetworkService serverService = createService(c ->
            c.withConnector(new NetworkConnectorConfig<String>()
                .withProtocol("test")
                .withMessageCodec(createStringCodecFactory())
                .withServerHandler((message, from) -> from.send(message + "-response"))
            ));

        NettyNetworkService clientService = createService(c ->
            c.withConnector(new NetworkConnectorConfig<String>()
                .withProtocol("test")
                .withMessageCodec(createStringCodecFactory())
            ));

        InetSocketAddress bindAddr = serverService.bind(new NetworkBindCallback() {
            // No-op.
        }).get().address();

        clientService.bind(new NetworkBindCallback() {
            // No-op.
        }).get();

        InetSocketAddress connectAddr = new InetSocketAddress(InetAddress.getLocalHost(), bindAddr.getPort());

        serverService.start();
        clientService.start();

        NetworkClient<Object> client = clientService.connector("test").newClient();

        NetworkClientCallbackMock<Object> callback = new NetworkClientCallbackMock<>();

        client.connect(connectAddr, callback).get();

        assertSame(NetworkClient.State.CONNECTED, client.state());

        clientService.stop();

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        try {
            client.connect(connectAddr, callback).get(1, TimeUnit.SECONDS);

            fail("Error was expected.");
        } catch (IllegalStateException e) {
            assertEquals("I/O thread pool terminated.", e.getMessage());
        }

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        callback.assertConnects(1);
        callback.assertDisconnects(1);
        callback.assertErrors(0);
    }

    @Test
    public void testPreConfiguredConnectorAndServerHandler() throws Exception {
        String protocol = "pre-configured";

        NettyNetworkService service = createService(c ->
            c.withConnector(new NetworkConnectorConfig<String>()
                .withProtocol(protocol)
                .withMessageCodec(createStringCodecFactory())
                .withServerHandler((message, from) -> from.send(message.decode() + "-response"))
            ));

        InetSocketAddress bindAddr = service.bind(new NetworkBindCallback() {
            // No-op.
        }).get().address();

        service.start();

        InetSocketAddress connectAddr = new InetSocketAddress(InetAddress.getLocalHost(), bindAddr.getPort());

        NetworkConnector<String> connector = service.connector(protocol);

        NetworkClient<String> client = connector.newClient();

        try {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(connectAddr, callback);

            client.send("test");

            callback.awaitForMessages("test-response");
        } finally {
            client.disconnect();
        }
    }

    @Test
    public void testPortAutoIncrement() throws Exception {
        repeat(5, i -> {
            NettyNetworkService service = createService(f -> {
                f.setPort(20100);
                f.setPortRange(10);
            });

            InetSocketAddress addr = service.bind(new NetworkBindCallback() {
                // No-op.
            }).get().address();

            assertEquals(20100 + i, addr.getPort());
        });

        say("Will fail.");

        NettyNetworkService failingService = createService(f -> {
            f.setPort(20100);
            f.setPortRange(5);
        });

        try {
            failingService.bind(new NetworkBindCallback() {
                // No-op.
            }).get();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(ErrorUtils.isCausedBy(IOException.class, e));
        }
    }

    private NettyNetworkService createService() {
        return createService(null);
    }

    private NettyNetworkService createService(Configurer configurer) {
        NetworkServiceFactory cfg = new NetworkServiceFactory();

        cfg.setTransport(context().transport());
        context().ssl().ifPresent(cfg::setSsl);
        cfg.setConnectTimeout(500);

        if (configurer != null) {
            configurer.configure(cfg);
        }

        // Important to reset test-dependent options after applying the configurer.
        cfg.setTransport(context().transport());
        context().ssl().ifPresent(cfg::setSsl);

        NettyNetworkService service = new NettyNetworkService(cfg);

        services.add(service);

        return service;
    }
}
