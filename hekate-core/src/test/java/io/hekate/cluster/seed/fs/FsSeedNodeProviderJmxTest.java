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

package io.hekate.cluster.seed.fs;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.test.TestUtils;
import java.io.File;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;

public class FsSeedNodeProviderJmxTest extends HekateNodeTestBase {
    private File tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tempDir = TestUtils.createTempDir();
    }

    @Override
    public void tearDown() throws Exception {
        TestUtils.deleteDir(tempDir);

        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        FsSeedNodeProviderConfig cfg = new FsSeedNodeProviderConfig();

        cfg.setWorkDir(tempDir);
        cfg.setCleanupInterval(100);

        FsSeedNodeProvider seedNodeProvider = new FsSeedNodeProvider(cfg);

        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(ClusterServiceFactory.class, cluster ->
                cluster.withSeedNodeProvider(seedNodeProvider)
            );
        }).join();

        ObjectName name = node.get(JmxService.class).nameFor(FsSeedNodeProviderJmx.class);

        assertEquals(tempDir.getCanonicalPath(), jmxAttribute(name, "WorkDir", String.class, node));
        assertEquals(100, (long)jmxAttribute(name, "CleanupInterval", Long.class, node));
    }
}
