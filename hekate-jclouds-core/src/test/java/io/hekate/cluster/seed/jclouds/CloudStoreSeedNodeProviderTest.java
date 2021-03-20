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

package io.hekate.cluster.seed.jclouds;

import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.core.report.DefaultConfigReporter;
import java.util.Collection;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static io.hekate.core.internal.util.Utils.NL;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CloudStoreSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<CloudStoreSeedNodeProvider> {
    private final CloudTestContext testCtx;

    public CloudStoreSeedNodeProviderTest(CloudTestContext testCtx) {
        this.testCtx = testCtx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<CloudTestContext> getCloudTestContexts() {
        return CloudTestContext.allContexts();
    }

    @BeforeClass
    public static void mayBeDisableTest() {
        // Disable if there are no cloud providers that are configured for tests.
        Assume.assumeFalse(getCloudTestContexts().isEmpty());
    }

    @Test
    public void testConfigReport() throws Exception {
        CloudStoreSeedNodeProvider provider = createProvider();

        assertEquals(
            NL
                + "  cloud-store:" + NL
                + "    provider: " + provider.provider() + NL
                + "    container: " + provider.container() + NL
                + "    properties: " + provider.properties() + NL
                + "    cleanup-interval: " + provider.cleanupInterval() + NL,
            DefaultConfigReporter.report(provider)
        );
    }

    @Override
    protected CloudStoreSeedNodeProvider createProvider() throws Exception {
        CloudStoreSeedNodeProviderConfig cfg = new CloudStoreSeedNodeProviderConfig()
            .withProvider(testCtx.storeProvider())
            .withContainer(testCtx.storeBucket())
            .withConnectTimeout(3000)
            .withSoTimeout(3000)
            .withCredentials(new BasicCredentialsSupplier()
                .withIdentity(testCtx.identity())
                .withCredential(testCtx.credential())
            )
            .withCleanupInterval(100);

        return new CloudStoreSeedNodeProvider(cfg);
    }
}
