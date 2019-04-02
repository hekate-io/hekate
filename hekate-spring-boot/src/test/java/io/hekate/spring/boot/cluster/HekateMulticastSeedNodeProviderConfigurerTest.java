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
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateMulticastSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class MulticastEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class MulticastDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.multicast.enable=true",
            "hekate.cluster.seed.multicast.interval=10",
            "hekate.cluster.seed.multicast.waitTime=20"
        }, MulticastEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof MulticastSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        MulticastSeedNodeProvider provider = (MulticastSeedNodeProvider)group.allProviders().get(0);

        assertEquals(10, provider.interval());
        assertEquals(20, provider.waitTime());
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.multicast.enable=false"
        }, MulticastDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof MulticastSeedNodeProvider);
    }
}
