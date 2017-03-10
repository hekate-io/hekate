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

package io.hekate.network.internal;

import io.hekate.HekateInstanceTestBase;
import io.hekate.core.HekateTestInstance;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NetworkServiceInstanceTest extends HekateInstanceTestBase {
    @Test
    public void testDefaultCodec() throws Exception {
        AtomicReference<CompletableFuture<String>> requestRef = new AtomicReference<>();

        HekateTestInstance receiver = createInstance(c ->
            c.findOrRegister(NetworkServiceFactory.class)
                .withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                    .withServerHandler((msg, from) -> {
                        requestRef.get().complete(msg.decode());

                        from.send(msg.decode() + "-response");
                    })
                )
        );

        HekateTestInstance sender = createInstance(c ->
            c.findOrRegister(NetworkServiceFactory.class)
                .withConnector(new NetworkConnectorConfig<String>()
                    .withProtocol("test")
                )
        );

        repeat(3, i -> {
            receiver.join();
            sender.join();

            CompletableFuture<String> response = new CompletableFuture<>();
            requestRef.set(new CompletableFuture<>());

            NetworkClient<String> client = sender.get(NetworkService.class).<String>get("test").newClient();

            client.connect(receiver.getSocketAddress(), (message, self) ->
                response.complete(message.decode())
            ).get(3, TimeUnit.SECONDS);

            client.send("request" + i);

            assertEquals("request" + i, requestRef.get().get(3, TimeUnit.SECONDS));
            assertEquals("request" + i + "-response", response.get(3, TimeUnit.SECONDS));

            sender.leave();
            receiver.leave();
        });
    }
}
