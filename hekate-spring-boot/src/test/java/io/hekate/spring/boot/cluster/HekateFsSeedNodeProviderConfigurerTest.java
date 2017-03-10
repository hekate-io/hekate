/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import java.io.File;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HekateFsSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    static class FsEnabledConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    static class FsDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private File tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("hekate_fs_seed").toFile();
    }

    @Override
    public void tearDown() throws Exception {
        deleteDirectory(tempDir);

        super.tearDown();
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.filesystem.enable:true",
            "hekate.cluster.seed.filesystem.work-dir:" + tempDir.getAbsolutePath()
        }, FsEnabledConfig.class);

        FsSeedNodeProvider provider = (FsSeedNodeProvider)getNode().get(DefaultClusterService.class).getSeedNodeProvider();

        assertEquals(tempDir.getAbsolutePath(), provider.getWorkDir().getAbsolutePath());
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.filesystem.enable:false"
        }, FsDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).getSeedNodeProvider();

        assertFalse(provider instanceof FsSeedNodeProvider);
    }
}
