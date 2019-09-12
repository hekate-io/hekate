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
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.test.TestUtils;
import java.io.File;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateFsSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class FsEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class FsDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private File tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("hekate_fs_seed").toFile();
    }

    @Override
    public void tearDown() throws Exception {
        TestUtils.deleteDir(tempDir);

        super.tearDown();
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.filesystem.enable=true",
            "hekate.cluster.seed.filesystem.work-dir=" + tempDir.getAbsolutePath()
        }, FsEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof FsSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());

        FsSeedNodeProvider provider = (FsSeedNodeProvider)group.allProviders().get(0);

        assertEquals(tempDir.getCanonicalPath(), provider.getWorkDir().getCanonicalPath());
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.filesystem.enable=false"
        }, FsDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof FsSeedNodeProvider);
    }
}
