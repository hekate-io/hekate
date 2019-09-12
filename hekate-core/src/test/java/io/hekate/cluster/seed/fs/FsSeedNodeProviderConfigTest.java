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

import io.hekate.HekateTestBase;
import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class FsSeedNodeProviderConfigTest extends HekateTestBase {
    private final FsSeedNodeProviderConfig config = new FsSeedNodeProviderConfig();

    @Test
    public void testCleanupInterval() {
        assertEquals(FsSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, config.getCleanupInterval());

        config.setCleanupInterval(1000000);

        assertEquals(1000000, config.getCleanupInterval());

        assertSame(config, config.withCleanupInterval(2000000));

        assertEquals(2000000, config.getCleanupInterval());
    }

    @Test
    public void testWorkDir() {
        assertNull(config.getWorkDir());

        File dir1 = new File(".");
        File dir2 = new File(".");

        config.setWorkDir(dir1);

        assertSame(dir1, config.getWorkDir());

        assertSame(config, config.withWorkDir(dir2));

        assertSame(dir2, config.getWorkDir());
    }
}
