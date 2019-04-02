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

package io.hekate.lock;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class LockRegionConfigTest extends HekateTestBase {
    private final LockRegionConfig cfg = new LockRegionConfig();

    @Test
    public void testName() {
        assertNull(cfg.getName());

        cfg.setName("test_1");

        assertEquals("test_1", cfg.getName());

        assertSame(cfg, cfg.withName("test_2"));

        assertEquals("test_2", cfg.getName());
    }
}
