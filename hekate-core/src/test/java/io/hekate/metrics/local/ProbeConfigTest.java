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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ProbeConfigTest extends MetricConfigTestBase {
    private ProbeConfig cfg = new ProbeConfig();

    @Test
    public void testProbe() {
        assertNull(cfg.getProbe());

        Probe probe1 = () -> 0;

        cfg.setProbe(probe1);

        assertSame(probe1, cfg.getProbe());

        cfg.setProbe(null);

        assertNull(cfg.getProbe());

        Probe probe2 = () -> 0;

        assertSame(cfg, cfg.withProbe(probe2));

        assertSame(probe2, cfg.getProbe());
    }

    @Test
    public void testInitValue() {
        assertEquals(0, cfg.getInitValue());

        cfg.setInitValue(1000);

        assertEquals(1000, cfg.getInitValue());

        assertSame(cfg, cfg.withInitValue(2000));

        assertEquals(2000, cfg.getInitValue());
    }

    @Test
    public void testConstructWithName() {
        cfg = new ProbeConfig("test1");

        assertEquals("test1", cfg.getName());

        cfg.setName(null);

        assertNull(cfg.getName());
    }

    @Override
    protected MetricConfigBase<?> createConfig() {
        return new ProbeConfig();
    }
}
