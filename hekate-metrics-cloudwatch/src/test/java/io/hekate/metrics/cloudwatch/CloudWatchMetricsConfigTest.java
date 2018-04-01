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

package io.hekate.metrics.cloudwatch;

import io.hekate.HekateTestBase;
import io.hekate.metrics.MetricFilter;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CloudWatchMetricsConfigTest extends HekateTestBase {
    private CloudWatchMetricsConfig cfg = new CloudWatchMetricsConfig();

    @Test
    public void testNamespace() {
        assertEquals(CloudWatchMetricsConfig.DEFAULT_NAMESPACE, cfg.getNamespace());

        cfg.setNamespace("Test");

        assertEquals("Test", cfg.getNamespace());

        cfg.setNamespace("Test2");

        assertEquals("Test2", cfg.getNamespace());

        assertSame(cfg, cfg.withNamespace("Test"));

        assertEquals("Test", cfg.getNamespace());
    }

    @Test
    public void testRegion() {
        assertNull(cfg.getRegion());

        cfg.setRegion("Test");

        assertEquals("Test", cfg.getRegion());

        cfg.setRegion(null);

        assertNull(cfg.getRegion());

        assertSame(cfg, cfg.withRegion("Test"));

        assertEquals("Test", cfg.getRegion());
    }

    @Test
    public void testPublishInterval() {
        assertEquals(CloudWatchMetricsConfig.DEFAULT_PUBLISH_INTERVAL, cfg.getPublishInterval());

        cfg.setPublishInterval(100500);

        assertEquals(100500, cfg.getPublishInterval());

        assertSame(cfg, cfg.withPublishInterval(5010));

        assertEquals(5010, cfg.getPublishInterval());
    }

    @Test
    public void testAccessKey() {
        assertNull(cfg.getAccessKey());

        cfg.setAccessKey("Test");

        assertEquals("Test", cfg.getAccessKey());

        cfg.setAccessKey(null);

        assertNull(cfg.getAccessKey());

        assertSame(cfg, cfg.withAccessKey("Test"));

        assertEquals("Test", cfg.getAccessKey());
    }

    @Test
    public void testSecretKey() {
        assertNull(cfg.getSecretKey());

        cfg.setSecretKey("Test");

        assertEquals("Test", cfg.getSecretKey());

        cfg.setSecretKey(null);

        assertNull(cfg.getSecretKey());

        assertSame(cfg, cfg.withSecretKey("Test"));

        assertEquals("Test", cfg.getSecretKey());
    }

    @Test
    public void testFilter() {
        assertNull(cfg.getFilter());

        MetricFilter f = metric -> true;

        cfg.setFilter(f);

        assertSame(f, cfg.getFilter());

        cfg.setFilter(null);

        assertNull(cfg.getFilter());

        assertSame(cfg, cfg.withFilter(f));

        assertSame(f, cfg.getFilter());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
