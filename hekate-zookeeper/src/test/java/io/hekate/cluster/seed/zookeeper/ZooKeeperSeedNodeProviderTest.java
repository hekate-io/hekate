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

package io.hekate.cluster.seed.zookeeper;

import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.core.HekateException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.function.Consumer;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZooKeeperSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<ZooKeeperSeedNodeProvider> {
    private static final int ZOOKEEPER_PORT = 10087;

    private static TestingServer server;

    @BeforeClass
    public static void startZooKeeper() throws Exception {
        server = new TestingServer(ZOOKEEPER_PORT);

        server.start();
    }

    @AfterClass
    public static void stopZooKeeper() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Override
    public void setUp() throws Exception {
        ignoreGhostThreads();

        super.setUp();
    }

    @Test
    public void testConnectTimeout() throws Exception {
        try (ServerSocket sock = new ServerSocket()) {
            InetSocketAddress addr = newSocketAddress();

            sock.bind(addr);

            ZooKeeperSeedNodeProvider provider = createProvider(cfg ->
                cfg.withConnectionString(addr.getHostString() + ':' + addr.getPort())
            );

            expectCause(HekateException.class, "Timeout connecting to ZooKeeper", () ->
                provider.startDiscovery(CLUSTER_1, newSocketAddress())
            );
        }
    }

    @Test
    public void testProvider() throws Exception {
        ZooKeeperSeedNodeProvider p1 = createProvider();

        assertEquals(server.getConnectString(), p1.connectionString());
        assertEquals("/hekate-test-path/", p1.basePath());

        ZooKeeperSeedNodeProvider p2 = createProvider(cfg -> cfg.withBasePath("/hekate-test-path"));

        assertEquals("/hekate-test-path/", p2.basePath());
    }

    @Override
    protected ZooKeeperSeedNodeProvider createProvider() throws Exception {
        return createProvider(null);
    }

    protected ZooKeeperSeedNodeProvider createProvider(Consumer<ZooKeeperSeedNodeProviderConfig> configurer) throws Exception {
        ZooKeeperSeedNodeProviderConfig cfg = new ZooKeeperSeedNodeProviderConfig()
            .withConnectionString(server.getConnectString())
            .withConnectTimeout(1000)
            .withSessionTimeout(1000)
            .withCleanupInterval(100)
            .withBasePath("/hekate-test-path/");

        if (configurer != null) {
            configurer.accept(cfg);
        }

        return new ZooKeeperSeedNodeProvider(cfg);
    }
}
