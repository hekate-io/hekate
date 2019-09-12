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

package io.hekate.network;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class NetworkSslConfigTest extends HekateTestBase {
    private final NetworkSslConfig cfg = new NetworkSslConfig();

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }

    @Test
    public void testProvider() {
        assertSame(NetworkSslConfig.Provider.AUTO, cfg.getProvider());

        cfg.setProvider(NetworkSslConfig.Provider.JDK);

        assertSame(NetworkSslConfig.Provider.JDK, cfg.getProvider());

        cfg.setProvider(null);

        assertNull(cfg.getProvider());

        assertSame(cfg, cfg.withProvider(NetworkSslConfig.Provider.OPEN_SSL));
        assertSame(NetworkSslConfig.Provider.OPEN_SSL, cfg.getProvider());
    }

    @Test
    public void testKeyStoreAlgorithm() {
        assertNull(cfg.getKeyStoreAlgorithm());

        cfg.setKeyStoreAlgorithm("test");

        assertEquals("test", cfg.getKeyStoreAlgorithm());

        cfg.setKeyStoreAlgorithm(null);

        assertNull(cfg.getKeyStoreAlgorithm());

        assertSame(cfg, cfg.withKeyStoreAlgorithm("test"));
        assertEquals("test", cfg.getKeyStoreAlgorithm());
    }

    @Test
    public void testKeyStorePath() {
        assertNull(cfg.getKeyStorePath());

        cfg.setKeyStorePath("test");

        assertEquals("test", cfg.getKeyStorePath());

        cfg.setKeyStorePath(null);

        assertNull(cfg.getKeyStorePath());

        assertSame(cfg, cfg.withKeyStorePath("test"));
        assertEquals("test", cfg.getKeyStorePath());
    }

    @Test
    public void testKeyStoreType() {
        assertNull(cfg.getKeyStoreType());

        cfg.setKeyStoreType("test");

        assertEquals("test", cfg.getKeyStoreType());

        cfg.setKeyStoreType(null);

        assertNull(cfg.getKeyStoreType());

        assertSame(cfg, cfg.withKeyStoreType("test"));
        assertEquals("test", cfg.getKeyStoreType());
    }

    @Test
    public void testKeyStorePassword() {
        assertNull(cfg.getKeyStorePassword());

        cfg.setKeyStorePassword("test");

        assertEquals("test", cfg.getKeyStorePassword());

        cfg.setKeyStorePassword(null);

        assertNull(cfg.getKeyStorePassword());

        assertSame(cfg, cfg.withKeyStorePassword("test"));
        assertEquals("test", cfg.getKeyStorePassword());
    }

    @Test
    public void testTrustStoreAlgorithm() {
        assertNull(cfg.getTrustStoreAlgorithm());

        cfg.setTrustStoreAlgorithm("test");

        assertEquals("test", cfg.getTrustStoreAlgorithm());

        cfg.setTrustStoreAlgorithm(null);

        assertNull(cfg.getTrustStoreAlgorithm());

        assertSame(cfg, cfg.withTrustStoreAlgorithm("test"));
        assertEquals("test", cfg.getTrustStoreAlgorithm());
    }

    @Test
    public void testTrustStorePath() {
        assertNull(cfg.getTrustStorePath());

        cfg.setTrustStorePath("test");

        assertEquals("test", cfg.getTrustStorePath());

        cfg.setTrustStorePath(null);

        assertNull(cfg.getTrustStorePath());

        assertSame(cfg, cfg.withTrustStorePath("test"));
        assertEquals("test", cfg.getTrustStorePath());
    }

    @Test
    public void testTrustStoreType() {
        assertNull(cfg.getTrustStoreType());

        cfg.setTrustStoreType("test");

        assertEquals("test", cfg.getTrustStoreType());

        cfg.setTrustStoreType(null);

        assertNull(cfg.getTrustStoreType());

        assertSame(cfg, cfg.withTrustStoreType("test"));
        assertEquals("test", cfg.getTrustStoreType());
    }

    @Test
    public void testTrustStorePassword() {
        assertNull(cfg.getTrustStorePassword());

        cfg.setTrustStorePassword("test");

        assertEquals("test", cfg.getTrustStorePassword());

        cfg.setTrustStorePassword(null);

        assertNull(cfg.getTrustStorePassword());

        assertSame(cfg, cfg.withTrustStorePassword("test"));
        assertEquals("test", cfg.getTrustStorePassword());
    }

    @Test
    public void testSslSessionCacheSize() {
        assertEquals(0, cfg.getSslSessionCacheSize());

        cfg.setSslSessionCacheSize(10);

        assertEquals(10, cfg.getSslSessionCacheSize());

        assertSame(cfg, cfg.withSslSessionCacheSize(1000));
        assertEquals(1000, cfg.getSslSessionCacheSize());
    }

    @Test
    public void testSslSessionCacheTimeout() {
        assertEquals(0, cfg.getSslSessionCacheTimeout());

        cfg.setSslSessionCacheTimeout(10);

        assertEquals(10, cfg.getSslSessionCacheTimeout());

        assertSame(cfg, cfg.withSslSessionCacheTimeout(1000));
        assertEquals(1000, cfg.getSslSessionCacheTimeout());
    }
}
