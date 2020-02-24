/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.cluster.seed.etcd;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateConfigurationException;
import io.hekate.util.format.ToString;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class EtcdSeedNodeProviderConfigTest extends HekateTestBase {
    private final EtcdSeedNodeProviderConfig cfg = new EtcdSeedNodeProviderConfig();

    @Test
    public void testEndpoints() {
        assertNull(cfg.getEndpoints());

        cfg.setEndpoints(new ArrayList<>(asList("http://host1:2379", "http://host2:2379")));

        assertEquals(asList("http://host1:2379", "http://host2:2379"), cfg.getEndpoints());

        assertSame(cfg, cfg.withEndpoint("http://host3:2379"));

        assertEquals(asList("http://host1:2379", "http://host2:2379", "http://host3:2379"), cfg.getEndpoints());

        assertSame(cfg, cfg.withEndpoints(asList("http://host1:2379", "http://host2:2379")));

        assertEquals(asList("http://host1:2379", "http://host2:2379"), cfg.getEndpoints());
    }

    @Test
    public void testEndpointsValidation() {
        String type = EtcdSeedNodeProviderConfig.class.getSimpleName();

        expectExactMessage(HekateConfigurationException.class, type + ": endpoints must be not null.", () ->
            fail(new EtcdSeedNodeProvider(cfg).toString())
        );

        expectExactMessage(HekateConfigurationException.class, type + ": endpoints must be not empty.", () ->
            fail(new EtcdSeedNodeProvider(cfg.withEndpoint(null)).toString())
        );

        expectExactMessage(HekateConfigurationException.class, type + ": endpoints must be not empty.", () ->
            fail(new EtcdSeedNodeProvider(cfg.withEndpoint("")).toString())
        );

        expect(HekateConfigurationException.class, type + ": " + URISyntaxException.class.getName(), () ->
            fail(new EtcdSeedNodeProvider(cfg.withEndpoint("invalid URI")).toString())
        );
    }

    @Test
    public void testUsername() {
        assertNull(cfg.getUsername());

        cfg.setUsername("test");

        assertEquals("test", cfg.getUsername());

        assertSame(cfg, cfg.withUsername("test2"));

        assertEquals("test2", cfg.getUsername());
    }

    @Test
    public void testPassword() {
        assertNull(cfg.getPassword());

        cfg.setPassword("test");

        assertEquals("test", cfg.getPassword());

        assertSame(cfg, cfg.withPassword("test2"));

        assertEquals("test2", cfg.getPassword());
    }

    @Test
    public void testBasePath() {
        assertEquals(EtcdSeedNodeProviderConfig.DEFAULT_BASE_PATH, cfg.getBasePath());

        cfg.setBasePath("/test/path");

        assertEquals("/test/path", cfg.getBasePath());

        assertSame(cfg, cfg.withBasePath("/test/path2"));

        assertEquals("/test/path2", cfg.getBasePath());
    }

    @Test
    public void testCleanupInterval() {
        assertEquals(EtcdSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, cfg.getCleanupInterval());

        cfg.setCleanupInterval(10001);

        assertEquals(10001, cfg.getCleanupInterval());

        assertSame(cfg, cfg.withCleanupInterval(10002));

        assertEquals(10002, cfg.getCleanupInterval());
    }

    @Test
    public void testToString() {
        cfg.setEndpoints(singletonList("http://localhost:2379"));
        cfg.setUsername("test");
        cfg.setPassword("test");

        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
