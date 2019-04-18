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
import io.hekate.cluster.seed.etcd.EtcdSeedNodeProvider;
import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import java.net.URI;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateEtcdSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class EtcdEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class EtcdDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private URI etcdUrl;

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("ETCD_ENABLED"));
    }

    @Before
    public void setUp() throws Exception {
        etcdUrl = new URI(HekateTestProps.get("ETCD_URL"));
    }

    @Test
    public void testEnabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(
            new String[]{
                "hekate.cluster.seed.etcd.enable=true",
                "hekate.cluster.seed.etcd.endpoints=" + etcdUrl + ',' + etcdUrl,
                "hekate.cluster.seed.etcd.base-path=/hekate-test"
            },
            EtcdEnabledConfig.class
        );

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof EtcdSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        EtcdSeedNodeProvider provider = (EtcdSeedNodeProvider)group.allProviders().get(0);

        assertEquals("/hekate-test", provider.basePath());
        assertEquals(asList(etcdUrl, etcdUrl), provider.endpoints());
    }

    @Test
    public void testDisabled() throws Exception {
        ignoreGhostThreads();

        registerAndRefresh(new String[]{
            "hekate.cluster.seed.etcd.enable=false"
        }, EtcdDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof ZooKeeperSeedNodeProvider);
    }
}
