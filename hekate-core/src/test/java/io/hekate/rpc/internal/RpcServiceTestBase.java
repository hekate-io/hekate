package io.hekate.rpc.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class RpcServiceTestBase extends HekateNodeTestBase {
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

    protected ClientAndServer prepareClientAndServer(Object rpc) throws Exception {
        HekateTestNode server = createNode(boot -> {
            boot.withNodeName("rpc-server");
            boot.withService(RpcServiceFactory.class, f ->
                f.withServer(new RpcServerConfig()
                    .withHandler(rpc)
                )
            );
        }).join();

        HekateTestNode client = createNode(boot -> boot.withNodeName("rpc-client")).join();

        awaitForTopology(client, server);

        return new ClientAndServer(client, server);
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
