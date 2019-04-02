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

package io.hekate.cluster.seed.fs;

import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.test.TestUtils;
import java.io.File;
import java.net.InetSocketAddress;
import org.junit.Test;

import static io.hekate.core.internal.util.AddressUtils.FILE_PREFIX;
import static io.hekate.core.internal.util.AddressUtils.PORT_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FsSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<FsSeedNodeProvider> {
    private File tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tempDir = TestUtils.createTempDir();
    }

    @Override
    public void tearDown() throws Exception {
        TestUtils.deleteDir(tempDir);

        super.tearDown();
    }

    @Test
    public void testInvalidFileNames() throws Exception {
        FsSeedNodeProvider provider = createProvider();

        InetSocketAddress addr = newSocketAddress();

        provider.startDiscovery(CLUSTER_1, addr);

        // Prefix only.
        File invalid = new File(provider.clusterDir(CLUSTER_1), FILE_PREFIX);

        invalid.createNewFile();

        assertEquals(1, provider.findSeedNodes(CLUSTER_1).size());
        assertTrue(provider.findSeedNodes(CLUSTER_1).contains(addr));

        invalid.delete();

        // No port.
        invalid = new File(provider.clusterDir(CLUSTER_1), FILE_PREFIX + addr.getAddress().getHostAddress());

        invalid.createNewFile();

        assertEquals(1, provider.findSeedNodes(CLUSTER_1).size());
        assertTrue(provider.findSeedNodes(CLUSTER_1).contains(addr));

        invalid.delete();

        // Invalid host.
        invalid = new File(provider.clusterDir(CLUSTER_1), FILE_PREFIX + "invalid#host" + PORT_SEPARATOR + "12134");

        invalid.createNewFile();

        assertEquals(1, provider.findSeedNodes(CLUSTER_1).size());
        assertTrue(provider.findSeedNodes(CLUSTER_1).contains(addr));

        invalid.delete();

        // Invalid port.
        invalid = new File(provider.clusterDir(CLUSTER_1), FILE_PREFIX + AddressUtils.host(addr) + PORT_SEPARATOR + "invalid_port");

        invalid.createNewFile();

        assertEquals(1, provider.findSeedNodes(CLUSTER_1).size());
        assertTrue(provider.findSeedNodes(CLUSTER_1).contains(addr));

        invalid.delete();
    }

    @Override
    protected FsSeedNodeProvider createProvider() throws Exception {
        FsSeedNodeProviderConfig cfg = new FsSeedNodeProviderConfig();

        cfg.setWorkDir(tempDir);
        cfg.setCleanupInterval(100);

        return new FsSeedNodeProvider(cfg);
    }
}
