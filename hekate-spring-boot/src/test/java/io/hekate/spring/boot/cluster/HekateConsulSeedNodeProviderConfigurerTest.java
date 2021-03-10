/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.HekateTestProps;
import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.consul.ConsulSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import java.net.URI;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateConsulSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class ConsulEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class ConsulDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private URI consulUrl;

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("CONSUL_ENABLED"));
    }

    @Before
    public void setUp() throws Exception {
        consulUrl = new URI(HekateTestProps.get("CONSUL_URL"));
    }

    @Test
    public void testEnabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(
            new String[]{
                "hekate.cluster.seed.consul.enable=true",
                "hekate.cluster.seed.consul.url=" + consulUrl,
                "hekate.cluster.seed.consul.base-path=///////hekate-test"
            },
            HekateConsulSeedNodeProviderConfigurerTest.ConsulEnabledConfig.class
        );

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof ConsulSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        ConsulSeedNodeProvider provider = (ConsulSeedNodeProvider)group.allProviders().get(0);

        assertEquals("hekate-test", provider.basePath());
        assertEquals(consulUrl, provider.url());
    }

    @Test
    public void testDisabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(new String[]{
            "hekate.cluster.seed.consul.enable=false"
        }, ConsulDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof ConsulSeedNodeProvider);
    }
}
