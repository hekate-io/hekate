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

package io.hekate.cluster.seed.consul;

import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.core.report.DefaultConfigReporter;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConsulSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<ConsulSeedNodeProvider> {
    private static String url;

    @BeforeClass
    public static void prepareTestClass() {
        // May be disable the whole test class.
        Assume.assumeTrue(HekateTestProps.is("CONSUL_ENABLED"));

        url = HekateTestProps.get("CONSUL_URL");
    }

    @Test
    public void testConfigReport() throws Exception {
        ConsulSeedNodeProvider provider = createProvider();

        assertEquals(
            "\n"
                + "  consul:\n"
                + "    url: " + provider.url() + "\n"
                + "    base-path: " + provider.basePath() + "\n"
                + "    cleanup-interval: " + provider.cleanupInterval() + "\n"
                + "    connect-timeout: " + provider.connectTimeout() + "\n"
                + "    read-timeout: " + provider.readTimeout() + "\n"
                + "    write-timeout: " + provider.writeTimeout() + "\n",
            DefaultConfigReporter.report(provider)
        );
    }

    @Override
    protected ConsulSeedNodeProvider createProvider() throws Exception {
        ConsulSeedNodeProviderConfig cfg = new ConsulSeedNodeProviderConfig();

        cfg.setUrl(url);
        cfg.setCleanupInterval(100);
        cfg.setBasePath("hekate/test");
        cfg.setConnectTimeout(3000L);
        cfg.setReadTimeout(3000L);
        cfg.setWriteTimeout(3000L);

        return new ConsulSeedNodeProvider(cfg);
    }
}
