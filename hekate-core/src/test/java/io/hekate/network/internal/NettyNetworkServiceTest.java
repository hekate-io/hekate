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

package io.hekate.network.internal;

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.test.NetworkClientCallbackMock;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NettyNetworkServiceTest extends HekateNodeParamTestBase {
    public NettyNetworkServiceTest(HekateTestContext textContext) {
        super(textContext);
    }

    @Test
    public void testClientAfterServiceStop() throws Exception {
        HekateTestNode serverNode = createNode(boot ->
            boot.withService(NetworkServiceFactory.class, net -> {
                net.withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                    .withServerHandler((message, from) ->
                        from.send(message.decode() + "-response")
                    )
                );
            })
        ).join();

        HekateTestNode clientNode = createNode(boot ->
            boot.withService(NetworkServiceFactory.class, net -> {
                net.withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                );
            })
        ).join();

        NetworkClient<Object> client = clientNode.network().connector("test").newClient();

        NetworkClientCallbackMock<Object> callback = new NetworkClientCallbackMock<>();

        client.connect(serverNode.localNode().socket(), callback).get();

        assertSame(NetworkClient.State.CONNECTED, client.state());

        clientNode.leave();

        assertSame(NetworkClient.State.DISCONNECTED, client.state());

        try {
            client.connect(serverNode.localNode().socket(), callback).get(1, TimeUnit.SECONDS);

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

        HekateTestNode node = createNode(boot ->
            boot.withService(NetworkServiceFactory.class, net -> {
                net.withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol(protocol)
                    .withServerHandler((message, from) ->
                        from.send(message.decode() + "-response")
                    )
                );
            })
        ).join();

        NetworkConnector<String> connector = node.network().connector(protocol);

        NetworkClient<String> client = connector.newClient();

        try {
            NetworkClientCallbackMock<String> callback = new NetworkClientCallbackMock<>();

            client.connect(node.localNode().socket(), callback);

            client.send("test");

            callback.awaitForMessages("test-response");
        } finally {
            client.disconnect();
        }
    }

    @Test
    public void testPortAutoIncrement() throws Exception {
        repeat(3, i -> {
            HekateTestNode node = createNode(boot ->
                boot.withService(NetworkServiceFactory.class, net -> {
                    net.setPort(20100);
                    net.setPortRange(10);
                })
            ).join();

            assertEquals(20100 + i, node.localNode().address().port());
        });

        say("Will fail.");

        try {
            createNode(boot ->
                boot.withService(NetworkServiceFactory.class, net -> {
                    net.setPort(20100);
                    net.setPortRange(3);
                })
            ).join();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(ErrorUtils.isCausedBy(IOException.class, e));
        }
    }
}
