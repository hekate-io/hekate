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

package io.hekate.cluster.seed.multicast;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

public class MulticastSeedNodeProviderConfigTest extends HekateTestBase {
    private final MulticastSeedNodeProviderConfig cfg = new MulticastSeedNodeProviderConfig();

    @Test
    public void testGroup() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_GROUP, cfg.getGroup());

        assertNotEquals("224.12.12.12", MulticastSeedNodeProviderConfig.DEFAULT_GROUP);

        cfg.setGroup("224.12.12.12");

        assertEquals("224.12.12.12", cfg.getGroup());

        assertSame(cfg, cfg.withGroup("224.12.12.13"));

        assertEquals("224.12.12.13", cfg.getGroup());
    }

    @Test
    public void testPort() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_PORT, cfg.getPort());

        cfg.setPort(10001);

        assertEquals(10001, cfg.getPort());

        assertSame(cfg, cfg.withPort(10002));

        assertEquals(10002, cfg.getPort());
    }

    @Test
    public void testInterval() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_INTERVAL, cfg.getInterval());

        cfg.setInterval(10001);

        assertEquals(10001, cfg.getInterval());

        assertSame(cfg, cfg.withInterval(10002));

        assertEquals(10002, cfg.getInterval());
    }

    @Test
    public void testWaitTime() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_WAIT_TIME, cfg.getWaitTime());

        cfg.setWaitTime(10001);

        assertEquals(10001, cfg.getWaitTime());

        assertSame(cfg, cfg.withWaitTime(10002));

        assertEquals(10002, cfg.getWaitTime());
    }

    @Test
    public void testTtl() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_TTL, cfg.getTtl());

        cfg.setTtl(10001);

        assertEquals(10001, cfg.getTtl());

        assertSame(cfg, cfg.withTtl(10002));

        assertEquals(10002, cfg.getTtl());
    }

    @Test
    public void testLoopBackDisabled() {
        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_LOOP_BACK_DISABLED, cfg.isLoopBackDisabled());

        cfg.setLoopBackDisabled(!MulticastSeedNodeProviderConfig.DEFAULT_LOOP_BACK_DISABLED);

        assertEquals(!MulticastSeedNodeProviderConfig.DEFAULT_LOOP_BACK_DISABLED, cfg.isLoopBackDisabled());

        assertSame(cfg, cfg.withLoopBackDisabled(MulticastSeedNodeProviderConfig.DEFAULT_LOOP_BACK_DISABLED));

        assertEquals(MulticastSeedNodeProviderConfig.DEFAULT_LOOP_BACK_DISABLED, cfg.isLoopBackDisabled());
    }
}
