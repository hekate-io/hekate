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

package io.hekate.metrics.statsd;

import io.hekate.HekateTestBase;
import io.hekate.metrics.MetricFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class StatsdMetricsConfigTest extends HekateTestBase {
    private final StatsdMetricsConfig cfg = new StatsdMetricsConfig();

    @Test
    public void testHost() {
        assertNull(cfg.getHost());

        cfg.setHost("host1");

        assertEquals("host1", cfg.getHost());

        assertSame(cfg, cfg.withHost("host2"));

        assertEquals("host2", cfg.getHost());
    }

    @Test
    public void testPort() {
        assertEquals(StatsdMetricsConfig.DEFAULT_PORT, cfg.getPort());

        cfg.setPort(StatsdMetricsConfig.DEFAULT_PORT + 1000);

        assertEquals(StatsdMetricsConfig.DEFAULT_PORT + 1000, cfg.getPort());

        assertSame(cfg, cfg.withPort(StatsdMetricsConfig.DEFAULT_PORT + 2000));

        assertEquals(StatsdMetricsConfig.DEFAULT_PORT + 2000, cfg.getPort());
    }

    @Test
    public void testMaxQueueSize() {
        assertEquals(StatsdMetricsConfig.DEFAULT_QUEUE_SIZE, cfg.getMaxQueueSize());

        cfg.setMaxQueueSize(StatsdMetricsConfig.DEFAULT_QUEUE_SIZE + 1000);

        assertEquals(StatsdMetricsConfig.DEFAULT_QUEUE_SIZE + 1000, cfg.getMaxQueueSize());

        assertSame(cfg, cfg.withMaxQueueSize(StatsdMetricsConfig.DEFAULT_QUEUE_SIZE + 2000));

        assertEquals(StatsdMetricsConfig.DEFAULT_QUEUE_SIZE + 2000, cfg.getMaxQueueSize());
    }

    @Test
    public void testBatchSize() {
        assertEquals(StatsdMetricsConfig.DEFAULT_BATCH_SIZE, cfg.getBatchSize());

        cfg.setBatchSize(StatsdMetricsConfig.DEFAULT_BATCH_SIZE + 1000);

        assertEquals(StatsdMetricsConfig.DEFAULT_BATCH_SIZE + 1000, cfg.getBatchSize());

        assertSame(cfg, cfg.withBatchSize(StatsdMetricsConfig.DEFAULT_BATCH_SIZE + 2000));

        assertEquals(StatsdMetricsConfig.DEFAULT_BATCH_SIZE + 2000, cfg.getBatchSize());
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

    @Test
    public void testToString() throws Exception {
        assertTrue(cfg.toString(), cfg.toString().startsWith(StatsdMetricsConfig.class.getSimpleName()));
    }
}
