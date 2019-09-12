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
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProviderConfig;
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

public class HekateCloudStoreSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class CloudStoreEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class CloudStoreDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void testEnabled() throws Exception {
        String testContainer = HekateTestProps.get("AWS_TEST_BUCKET");
        String testIdentity = HekateTestProps.get("AWS_TEST_ACCESS_KEY");
        String testCredential = HekateTestProps.get("AWS_TEST_SECRET_KEY");
        String testRegion = HekateTestProps.get("AWS_TEST_REGION");

        registerAndRefresh(new String[]{
            "hekate.cluster=" + UUID.randomUUID().toString(),
            "hekate.cluster.seed.cloudstore.enable=true",
            "hekate.cluster.seed.cloudstore.provider=aws-s3",
            "hekate.cluster.seed.cloudstore.container=" + testContainer,
            "hekate.cluster.seed.cloudstore.identity=" + testIdentity,
            "hekate.cluster.seed.cloudstore.credential=" + testCredential,
            "hekate.cluster.seed.cloudstore.properties.jclouds.regions=" + testRegion,
        }, CloudStoreEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof CloudStoreSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        CloudStoreSeedNodeProviderConfig cfg = get(CloudStoreSeedNodeProviderConfig.class);

        assertEquals("aws-s3", cfg.getProvider());
        assertEquals(testContainer, cfg.getContainer());
        assertEquals(testIdentity, cfg.getCredentials().get().identity);
        assertEquals(testCredential, cfg.getCredentials().get().credential);
        assertEquals(testRegion, cfg.getProperties().getProperty("jclouds.regions"));
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.cloudstore.enable=false"
        }, CloudStoreDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof CloudStoreSeedNodeProvider);
    }
}
