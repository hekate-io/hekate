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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CounterConfigTest extends MetricConfigTestBase {
    private CounterConfig cfg = new CounterConfig();

    @Test
    public void testTotalName() {
        assertNull(cfg.getTotalName());

        cfg.setTotalName("test1");

        assertEquals("test1", cfg.getTotalName());

        cfg.setTotalName(null);

        assertNull(cfg.getTotalName());

        assertSame(cfg, cfg.withTotalName("test2"));

        assertEquals("test2", cfg.getTotalName());
    }

    @Test
    public void testAutoReset() {
        assertFalse(cfg.isAutoReset());

        cfg.setAutoReset(true);

        assertTrue(cfg.isAutoReset());

        assertSame(cfg, cfg.withAutoReset(false));

        assertFalse(cfg.isAutoReset());
    }

    @Test
    public void testConstructWithName() {
        cfg = new CounterConfig("test1");

        assertEquals("test1", cfg.getName());

        cfg.setName(null);

        assertNull(cfg.getName());
    }

    @Override
    protected MetricConfigBase<?> createConfig() {
        return new CounterConfig();
    }
}
