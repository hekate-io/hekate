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

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TimerConfigTest extends MetricConfigTestBase {
    private final TimerConfig cfg = new TimerConfig();

    @Test
    public void testTotalName() {
        assertNull(cfg.getRateName());

        cfg.setRateName("test1");

        assertEquals("test1", cfg.getRateName());

        cfg.setRateName(null);

        assertNull(cfg.getRateName());

        assertSame(cfg, cfg.withRateName("test2"));

        assertEquals("test2", cfg.getRateName());
    }

    @Test
    public void testTimeUnit() {
        assertSame(TimeUnit.NANOSECONDS, cfg.getTimeUnit());

        cfg.setTimeUnit(null);

        assertSame(TimeUnit.NANOSECONDS, cfg.getTimeUnit());

        assertSame(cfg, cfg.withTimeUnit(null));

        assertSame(TimeUnit.NANOSECONDS, cfg.getTimeUnit());

        cfg.setTimeUnit(TimeUnit.SECONDS);

        assertSame(TimeUnit.SECONDS, cfg.getTimeUnit());

        assertSame(cfg, cfg.withTimeUnit(TimeUnit.MINUTES));

        assertSame(TimeUnit.MINUTES, cfg.getTimeUnit());
    }

    @Override
    protected MetricConfigBase<?> createConfig() {
        return new TimerConfig();
    }

}
