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

package io.hekate.metrics.influxdb;

import io.hekate.HekateTestBase;
import io.hekate.metrics.MetricFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class InfluxDbMetricsConfigTest extends HekateTestBase {
    private final InfluxDbMetricsConfig cfg = new InfluxDbMetricsConfig();

    @Test
    public void testUrl() {
        assertNull(cfg.getUrl());

        cfg.setUrl("http://localhost:8086");

        assertEquals("http://localhost:8086", cfg.getUrl());

        assertSame(cfg, cfg.withUrl("http://localhost2:8086"));

        assertEquals("http://localhost2:8086", cfg.getUrl());
    }

    @Test
    public void testDatabase() {
        assertNull(cfg.getDatabase());

        cfg.setDatabase("db1");

        assertEquals("db1", cfg.getDatabase());

        assertSame(cfg, cfg.withDatabase("db2"));

        assertEquals("db2", cfg.getDatabase());
    }

    @Test
    public void testUser() {
        assertNull(cfg.getUser());

        cfg.setUser("u1");

        assertEquals("u1", cfg.getUser());

        assertSame(cfg, cfg.withUser("u2"));

        assertEquals("u2", cfg.getUser());
    }

    @Test
    public void testPassword() {
        assertNull(cfg.getPassword());

        cfg.setPassword("p1");

        assertEquals("p1", cfg.getPassword());

        assertSame(cfg, cfg.withPassword("p2"));

        assertEquals("p2", cfg.getPassword());
    }

    @Test
    public void testMaxQueueSize() {
        assertEquals(InfluxDbMetricsConfig.DEFAULT_QUEUE_SIZE, cfg.getMaxQueueSize());

        cfg.setMaxQueueSize(InfluxDbMetricsConfig.DEFAULT_QUEUE_SIZE + 1000);

        assertEquals(InfluxDbMetricsConfig.DEFAULT_QUEUE_SIZE + 1000, cfg.getMaxQueueSize());

        assertSame(cfg, cfg.withMaxQueueSize(InfluxDbMetricsConfig.DEFAULT_QUEUE_SIZE + 2000));

        assertEquals(InfluxDbMetricsConfig.DEFAULT_QUEUE_SIZE + 2000, cfg.getMaxQueueSize());
    }

    @Test
    public void testTimeout() {
        assertEquals(InfluxDbMetricsConfig.DEFAULT_TIMEOUT, cfg.getTimeout());

        cfg.setTimeout(InfluxDbMetricsConfig.DEFAULT_TIMEOUT + 1000);

        assertEquals(InfluxDbMetricsConfig.DEFAULT_TIMEOUT + 1000, cfg.getTimeout());

        assertSame(cfg, cfg.withTimeout(InfluxDbMetricsConfig.DEFAULT_TIMEOUT + 2000));

        assertEquals(InfluxDbMetricsConfig.DEFAULT_TIMEOUT + 2000, cfg.getTimeout());
    }

    @Test
    public void testFilter() {
        assertNull(cfg.getFilter());

        MetricFilter f1 = metric -> true;
        MetricFilter f2 = metric -> true;

        cfg.setFilter(f1);

        assertSame(f1, cfg.getFilter());

        assertSame(cfg, cfg.withFilter(f2));

        assertSame(f2, cfg.getFilter());

        cfg.setFilter(null);

        assertNull(cfg.getFilter());
    }
}
