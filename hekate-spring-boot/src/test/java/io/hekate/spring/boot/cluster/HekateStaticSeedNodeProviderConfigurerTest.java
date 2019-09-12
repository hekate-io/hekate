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
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateStaticSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class EnabledConfig {
        // No-op.
    }

    @EnableHekate
    @EnableAutoConfiguration
    public static class DisabledConfig {
        // No-op.
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.static.enable=true",
            "hekate.cluster.seed.static.addresses=localhost:10012,localhost:10013"
        }, EnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof StaticSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        StaticSeedNodeProvider provider = (StaticSeedNodeProvider)group.allProviders().get(0);

        say("Addresses: " + provider.getAddresses());

        assertEquals(2, provider.getAddresses().size());
        assertTrue(provider.getAddresses().get(0).toString().contains(":10012"));
        assertTrue(provider.getAddresses().get(1).toString().contains(":10013"));
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.static.enable=false"
        }, DisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof StaticSeedNodeProvider);
    }
}
