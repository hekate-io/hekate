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

package io.hekate.cluster.seed.jclouds;

import io.hekate.util.format.ToString;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CloudStoreSeedNodeProviderConfigTest extends CloudPropertiesBaseTestBase {
    private final CloudStoreSeedNodeProviderConfig cfg = new CloudStoreSeedNodeProviderConfig();

    @Test
    public void testProvider() {
        assertNull(cfg.getProvider());

        cfg.setProvider("test");

        assertEquals("test", cfg.getProvider());

        cfg.setProvider(null);

        assertNull(cfg.getProvider());

        assertSame(cfg, cfg.withProvider("test"));

        assertEquals("test", cfg.getProvider());
    }

    @Test
    public void testCredentials() {
        BasicCredentialsSupplier credentials = new BasicCredentialsSupplier();

        assertNull(cfg.getCredentials());

        cfg.setCredentials(credentials);

        assertSame(credentials, cfg.getCredentials());

        cfg.setCredentials(null);

        assertNull(cfg.getCredentials());

        assertSame(cfg, cfg.withCredentials(credentials));

        assertSame(credentials, cfg.getCredentials());
    }

    @Test
    public void testProperties() {
        Properties props = new Properties();

        assertNull(cfg.getProperties());

        cfg.setProperties(props);

        assertSame(props, cfg.getProperties());

        cfg.setProperties(null);

        assertNull(cfg.getProperties());

        assertSame(cfg, cfg.withProperty("key", "value"));

        assertTrue(cfg.getProperties().containsKey("key"));
        assertTrue(cfg.getProperties().containsValue("value"));
    }

    @Test
    public void testCleanupInterval() {
        assertEquals(CloudStoreSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, cfg.getCleanupInterval());

        cfg.setCleanupInterval(10001);

        assertEquals(10001, cfg.getCleanupInterval());

        assertSame(cfg, cfg.withCleanupInterval(10002));

        assertEquals(10002, cfg.getCleanupInterval());
    }

    @Test
    public void testContainer() {
        assertNull(cfg.getContainer());

        cfg.setContainer("test");

        assertEquals("test", cfg.getContainer());

        cfg.setContainer(null);

        assertNull(cfg.getContainer());

        assertSame(cfg, cfg.withContainer("test"));

        assertEquals("test", cfg.getContainer());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }

    @Override
    protected CloudPropertiesBase createConfig() {
        return cfg;
    }
}
