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

package io.hekate.rpc.internal;

import io.hekate.HekateNodeMultiCodecTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class RpcServiceTestBase extends HekateNodeMultiCodecTestBase {
    protected static class ClientAndServer {
        private final HekateTestNode client;

        private final HekateTestNode server;

        public ClientAndServer(HekateTestNode client, HekateTestNode server) {
            this.server = server;
            this.client = client;
        }

        public HekateTestNode client() {
            return client;
        }

        public HekateTestNode server() {
            return server;
        }
    }

    protected static class ClientAndServers {
        private final HekateTestNode client;

        private final List<HekateTestNode> servers;

        public ClientAndServers(HekateTestNode client, List<HekateTestNode> servers) {
            this.servers = servers;
            this.client = client;
        }

        public HekateTestNode client() {
            return client;
        }

        public List<HekateTestNode> servers() {
            return servers;
        }
    }

    public RpcServiceTestBase(MultiCodecTestContext ctx) {
        super(ctx);
    }

    protected ClientAndServer prepareClientAndServer(Object rpc) throws Exception {
        return prepareClientAndServer(rpc, null);
    }

    protected ClientAndServer prepareClientAndServer(Object rpc, String tag) throws Exception {
        HekateTestNode server = prepareServer(rpc, tag);

        HekateTestNode client = createNode(boot -> boot.withNodeName("rpc-client")).join();

        awaitForTopology(client, server);

        return new ClientAndServer(client, server);
    }

    protected HekateTestNode prepareServer(Object rpc, String tag) throws Exception {
        return createNode(boot -> {
            boot.withNodeName("rpc-server");
            boot.withService(RpcServiceFactory.class, f ->
                f.withServer(new RpcServerConfig()
                    .withHandler(rpc)
                    .withTag(tag)
                )
            );
        }).join();
    }

    protected ClientAndServers prepareClientAndServers(Object... rpcHandlers) throws Exception {
        assertNotNull(rpcHandlers);
        assertTrue(rpcHandlers.length > 1);

        List<HekateTestNode> allServers = new ArrayList<>();

        for (Object rpc : rpcHandlers) {
            String nodeName = "rpc-server-" + (allServers.size() + 1);

            HekateTestNode server = createNode(boot -> {
                boot.withNodeName(nodeName);
                boot.withService(RpcServiceFactory.class, f ->
                    f.withServer(new RpcServerConfig()
                        .withHandler(rpc)
                    )
                );
            }).join();

            allServers.add(server);
        }

        HekateTestNode client = createNode(boot -> boot.withNodeName("rpc-client")).join();

        awaitForTopology(allServers, client);

        return new ClientAndServers(client, allServers);
    }
}
