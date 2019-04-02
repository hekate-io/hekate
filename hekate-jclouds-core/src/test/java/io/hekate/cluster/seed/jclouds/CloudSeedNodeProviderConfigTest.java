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
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CloudSeedNodeProviderConfigTest extends CloudPropertiesBaseTestBase {
    private final CloudSeedNodeProviderConfig cfg = new CloudSeedNodeProviderConfig();

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
    public void testEndpoint() {
        assertNull(cfg.getEndpoint());

        cfg.setEndpoint("test");

        assertEquals("test", cfg.getEndpoint());

        cfg.setEndpoint(null);

        assertNull(cfg.getEndpoint());

        assertSame(cfg, cfg.withEndpoint("test"));

        assertEquals("test", cfg.getEndpoint());
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
    public void testRegions() {
        assertNull(cfg.getRegions());

        cfg.setRegions(Collections.singleton("test1"));

        assertTrue(cfg.getRegions().contains("test1"));

        cfg.setRegions(null);

        assertNull(cfg.getRegions());

        assertSame(cfg, cfg.withRegion("test2"));

        assertTrue(cfg.getRegions().contains("test2"));
    }

    @Test
    public void testZones() {
        assertNull(cfg.getZones());

        cfg.setZones(Collections.singleton("test1"));

        assertTrue(cfg.getZones().contains("test1"));

        cfg.setZones(null);

        assertNull(cfg.getZones());

        assertSame(cfg, cfg.withZone("test2"));

        assertTrue(cfg.getZones().contains("test2"));
    }

    @Test
    public void testTags() {
        assertNull(cfg.getTags());

        cfg.setTags(Collections.singletonMap("test1", "1"));

        assertEquals("1", cfg.getTags().get("test1"));

        cfg.setTags(null);

        assertNull(cfg.getTags());

        assertSame(cfg, cfg.withTag("test2", "2"));

        assertEquals("2", cfg.getTags().get("test2"));
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
