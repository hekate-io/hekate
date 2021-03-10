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

package io.hekate.cluster.seed.etcd;

import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.core.HekateException;
import io.hekate.core.report.DefaultConfigReporter;
import java.net.InetSocketAddress;
import java.util.function.Consumer;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.hekate.core.internal.util.Utils.NL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class EtcdSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<EtcdSeedNodeProvider> {
    private static String url;

    @BeforeClass
    public static void prepareTestClass() {
        // May be disable the whole test class.
        Assume.assumeTrue(HekateTestProps.is("ETCD_ENABLED"));

        url = HekateTestProps.get("ETCD_URL");
    }

    @Override
    public void setUp() throws Exception {
        EtcdSeedNodeProvider provider = createProvider();

        provider.findSeedNodes(CLUSTER_1).forEach(address -> {
            try {
                provider.unregisterRemote(CLUSTER_1, address);
            } catch (HekateException e) {
                // No-op.
            }
        });

        provider.findSeedNodes(CLUSTER_2).forEach(address -> {
            try {
                provider.unregisterRemote(CLUSTER_2, address);
            } catch (HekateException e) {
                // No-op.
            }
        });

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        // Need to await for gRPC-related threads termination
        // as there seems to be no way to hook into the Etcd client's shutdown logic.
        awaitForNoSuchThread("grpc-default");
    }

    @Test
    public void testConfigReport() throws Exception {
        EtcdSeedNodeProvider provider = createProvider();

        assertEquals(
            NL
                + "  etcd:" + NL
                + "    endpoints: " + provider.endpoints() + NL
                + "    base-path: " + provider.basePath() + NL
                + "    cleanup-interval: " + provider.cleanupInterval() + NL,
            DefaultConfigReporter.report(provider)
        );
    }

    @Test
    public void testErrors() throws Exception {
        EtcdSeedNodeProvider provider = createProvider(cfg ->
            // Invalid URL.
            cfg.setEndpoints(singletonList("http://localhost:0000"))
        );

        InetSocketAddress address = newSocketAddress();

        expect(HekateException.class, () ->
            provider.startDiscovery(CLUSTER_1, address)
        );

        expect(HekateException.class, () ->
            provider.findSeedNodes(CLUSTER_1)
        );

        expect(HekateException.class, () ->
            provider.registerRemote(CLUSTER_1, address)
        );

        expect(HekateException.class, () ->
            provider.unregisterRemote(CLUSTER_1, address)
        );

        expect(HekateException.class, () ->
            provider.stopDiscovery(CLUSTER_1, address)
        );
    }

    @Override
    protected EtcdSeedNodeProvider createProvider() throws Exception {
        return createProvider(null);
    }

    private EtcdSeedNodeProvider createProvider(Consumer<EtcdSeedNodeProviderConfig> configurer) throws Exception {
        EtcdSeedNodeProviderConfig cfg = new EtcdSeedNodeProviderConfig()
            .withBasePath("/hekate/test")
            .withCleanupInterval(100)
            .withEndpoint(url);

        if (configurer != null) {
            configurer.accept(cfg);
        }

        return new EtcdSeedNodeProvider(cfg);
    }
}
