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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateZooKeeperSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class ZooKeeperEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class ZooKeeperDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private static TestingServer zooKeeper;

    @BeforeClass
    public static void startTestServer() throws Exception {
        zooKeeper = new TestingServer(true);
    }

    @AfterClass
    public static void stopTestServer() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    @Test
    public void testEnabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(new String[]{
            "hekate.cluster.seed.zookeeper.enable=true",
            "hekate.cluster.seed.zookeeper.connection-string=" + zooKeeper.getConnectString(),
            "hekate.cluster.seed.zookeeper.base-path=/hekate-test",
            "hekate.cluster.seed.zookeeper.connect-timeout=500",
            "hekate.cluster.seed.zookeeper.session-timeout=500"
        }, ZooKeeperEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof ZooKeeperSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        ZooKeeperSeedNodeProvider provider = (ZooKeeperSeedNodeProvider)group.allProviders().get(0);

        assertEquals(zooKeeper.getConnectString(), provider.connectionString());
        assertEquals("/hekate-test/", provider.basePath());
    }

    @Test
    public void testDisabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(new String[]{
            "hekate.cluster.seed.zookeeper.enable=false"
        }, ZooKeeperDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof ZooKeeperSeedNodeProvider);
    }
}
