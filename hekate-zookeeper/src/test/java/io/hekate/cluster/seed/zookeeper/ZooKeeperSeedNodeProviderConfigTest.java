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

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ZooKeeperSeedNodeProviderConfigTest extends HekateTestBase {
    private final ZooKeeperSeedNodeProviderConfig cfg = new ZooKeeperSeedNodeProviderConfig();

    @Test
    public void testConnectString() {
        assertNull(cfg.getConnectionString());

        cfg.setConnectionString("localhost:9999");

        assertEquals("localhost:9999", cfg.getConnectionString());

        cfg.setConnectionString(null);

        assertNull(cfg.getConnectionString());

        assertSame(cfg, cfg.withConnectionString("localhost:9999"));

        assertEquals("localhost:9999", cfg.getConnectionString());
    }

    @Test
    public void testConnectTimeout() {
        assertEquals(ZooKeeperSeedNodeProviderConfig.DEFAULT_CONNECT_TIMEOUT, cfg.getConnectTimeout());

        cfg.setConnectTimeout(100500);

        assertEquals(100500, cfg.getConnectTimeout());

        assertSame(cfg, cfg.withConnectTimeout(100));

        assertEquals(100, cfg.getConnectTimeout());
    }

    @Test
    public void testSessionTimeout() {
        assertEquals(ZooKeeperSeedNodeProviderConfig.DEFAULT_SESSION_TIMEOUT, cfg.getSessionTimeout());

        cfg.setSessionTimeout(100500);

        assertEquals(100500, cfg.getSessionTimeout());

        assertSame(cfg, cfg.withSessionTimeout(100));

        assertEquals(100, cfg.getSessionTimeout());
    }

    @Test
    public void testBasePath() {
        assertEquals(ZooKeeperSeedNodeProviderConfig.DEFAULT_BASE_PATH, cfg.getBasePath());

        cfg.setBasePath("/foo/bar");

        assertEquals("/foo/bar", cfg.getBasePath());

        cfg.setBasePath(null);

        assertNull(cfg.getBasePath());

        assertSame(cfg, cfg.withBasePath("/foo/bar/baz"));

        assertEquals("/foo/bar/baz", cfg.getBasePath());
    }

    @Test
    public void testCleanupInterval() {
        assertEquals(ZooKeeperSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, cfg.getCleanupInterval());

        cfg.setCleanupInterval(10001);

        assertEquals(10001, cfg.getCleanupInterval());

        assertSame(cfg, cfg.withCleanupInterval(10002));

        assertEquals(10002, cfg.getCleanupInterval());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
