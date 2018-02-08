/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.metrics.local;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public abstract class MetricConfigTestBase extends HekateTestBase {
    protected abstract MetricConfigBase<?> createConfig();

    @Test
    public void testName() {
        MetricConfigBase<?> cfg = createConfig();

        assertNull(cfg.getName());

        cfg.setName("test1");

        assertEquals("test1", cfg.getName());

        cfg.setName(null);

        assertNull(cfg.getName());

        assertSame(cfg, cfg.withName("test2"));

        assertEquals("test2", cfg.getName());
    }

    @Test
    public void testToString() {
        MetricConfigBase<?> cfg = createConfig();

        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
