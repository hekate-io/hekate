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
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkServiceFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NetworkServiceNodeTest extends HekateNodeParamTestBase {
    public NetworkServiceNodeTest(HekateTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testDefaultCodec() throws Exception {
        AtomicReference<CompletableFuture<String>> requestRef = new AtomicReference<>();

        HekateTestNode receiver = createNode(c ->
            c.withService(NetworkServiceFactory.class)
                .withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                    .withServerHandler((msg, from) -> {
                        requestRef.get().complete(msg.decode());

                        from.send(msg.decode() + "-response");
                    })
                )
        );

        HekateTestNode sender = createNode(c ->
            c.withService(NetworkServiceFactory.class)
                .withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                )
        );

        repeat(3, i -> {
            receiver.join();
            sender.join();

            CompletableFuture<String> response = new CompletableFuture<>();
            requestRef.set(new CompletableFuture<>());

            NetworkClient<String> client = sender.network().<String>connector("test").newClient();

            get(client.connect(receiver.localNode().socket(), (message, self) ->
                response.complete(message.decode())
            ));

            client.send("request" + i);

            assertEquals("request" + i, get(requestRef.get()));
            assertEquals("request" + i + "-response", get(response));

            sender.leave();
            receiver.leave();
        });
    }
}
