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

package io.hekate.javadoc.cluster.seed;

import io.hekate.HekateTestBase;
import io.hekate.HekateTestProps;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.SeedNodeProviderGroupConfig;
import io.hekate.cluster.seed.SeedNodeProviderGroupPolicy;
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProviderConfig;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProviderConfig;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.test.TestUtils;
import java.io.File;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SeedNodeProviderGroupJavadocTest extends HekateTestBase {
    private File tempDir;

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("MULTICAST_ENABLED"));
    }

    @Before
    public void setUp() throws Exception {
        tempDir = TestUtils.createTempDir();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDir(tempDir);
    }

    @Test
    public void exampleConfiguration() throws Exception {
        // Start:configuration
        // Prepare seed node providers group.
        SeedNodeProviderGroupConfig seedGroupCfg = new SeedNodeProviderGroupConfig()
            // First provider.
            .withProvider(
                new MulticastSeedNodeProvider(
                    new MulticastSeedNodeProviderConfig()
                        .withGroup("224.1.2.12")
                        .withPort(45454)
                        .withInterval(200)
                        .withWaitTime(1000)
                )
            )
            // Second provider.
            .withProvider(
                new FsSeedNodeProvider(
                    new FsSeedNodeProviderConfig().withWorkDir(tempDir)
                )
            )
            // Do not fail if at least one provider can find seed nodes.
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS);

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(ClusterServiceFactory.class, cluster -> {
                // Register the providers group to the cluster service.
                cluster.withSeedNodeProvider(new SeedNodeProviderGroup(seedGroupCfg));
            })
            .join();
        // End:configuration

        hekate.leave();
    }
}
