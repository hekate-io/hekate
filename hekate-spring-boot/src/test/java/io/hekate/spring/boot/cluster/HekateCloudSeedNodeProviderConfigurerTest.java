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

import io.hekate.HekateTestProps;
import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProviderConfig;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import java.util.UUID;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateCloudSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class CloudEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class CloudDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void testEnabled() throws Exception {
        String testIdentity = HekateTestProps.get("AWS_TEST_ACCESS_KEY");
        String testCredential = HekateTestProps.get("AWS_TEST_SECRET_KEY");
        String testRegion = HekateTestProps.get("AWS_TEST_REGION");
        String testEndpoint = "https://ec2." + testRegion + ".amazonaws.com";

        registerAndRefresh(new String[]{
            "hekate.cluster=" + UUID.randomUUID().toString(),
            "hekate.cluster.seed.cloud.enable=true",
            "hekate.cluster.seed.cloud.provider=aws-ec2",
            "hekate.cluster.seed.cloud.endpoint=" + testEndpoint,
            "hekate.cluster.seed.cloud.identity=" + testIdentity,
            "hekate.cluster.seed.cloud.credential=" + testCredential,
            "hekate.cluster.seed.cloud.regions=" + testRegion,
            "hekate.cluster.seed.cloud.zones=" + testRegion + "a, " + testRegion + 'b',
            "hekate.cluster.seed.cloud.tags.HEKATE=1",
            "hekate.cluster.seed.cloud.properties.test.property=test.value",
        }, CloudEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof CloudSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        CloudSeedNodeProviderConfig cfg = get(CloudSeedNodeProviderConfig.class);

        assertEquals("aws-ec2", cfg.getProvider());
        assertEquals(testEndpoint, cfg.getEndpoint());
        assertEquals(testIdentity, cfg.getCredentials().get().identity);
        assertEquals(testCredential, cfg.getCredentials().get().credential);
        assertTrue(cfg.getRegions().contains(testRegion));
        assertTrue(cfg.getZones().contains(testRegion + 'a'));
        assertTrue(cfg.getZones().contains(testRegion + 'b'));
        assertEquals("1", cfg.getTags().get("HEKATE"));
        assertEquals("test.value", cfg.getProperties().getProperty("test.property"));
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.cloud.enable=false"
        }, CloudDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof CloudSeedNodeProvider);
    }
}
