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

package io.hekate.metrics.local.internal;

import io.hekate.metrics.Metric;
import io.hekate.metrics.local.ProbeConfig;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalMetricsServiceProbeTest extends LocalMetricsServiceTestBase {
    @Test
    public void testProbeInitValue() throws Exception {
        metrics.register(new ProbeConfig("p").withInitValue(1000).withProbe(() -> 0));

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").name());
        assertEquals(1000, metrics.metric("p").value());
        assertEquals(1000, metrics.get("p"));
        assertEquals(1000, metrics.get("p", 10));
    }

    @Test
    public void testProbeDefaultValue() throws Exception {
        metrics.register(new ProbeConfig("p").withProbe(() -> 1000));

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").name());
        assertEquals(0, metrics.metric("p").value());
    }

    @Test
    public void testProbeUpdates() throws Exception {
        AtomicLong val = new AtomicLong(1000);

        metrics.register(new ProbeConfig("p1").withProbe(val::get));

        Metric metric = metrics.metric("p1");

        for (int i = 1; i <= 5; i++) {
            awaitForMetric(val.get(), metric);

            val.set(val.get() * i);
        }
    }

    @Test
    public void testProbeDisableOnError() throws Exception {
        AtomicLong val = new AtomicLong();

        CountDownLatch errorLatch = new CountDownLatch(1);

        metrics.register(new ProbeConfig("p1").withProbe(() -> {
            long v = val.incrementAndGet();

            if (v == 3) {
                errorLatch.countDown();

                throw TEST_ERROR;
            }

            return v;
        }));

        Metric metric = metrics.metric("p1");

        await(errorLatch);

        awaitForMetric(2, metric);

        sleep(TEST_METRICS_REFRESH_INTERVAL * 3);

        assertEquals(2, metrics.metric("p1").value());
    }

    @Test
    public void testMergeConfigurations() throws Exception {
        AtomicInteger probe = new AtomicInteger(1000);

        restart(c -> {
            c.withRefreshInterval(Long.MAX_VALUE);
            c.withMetric(new ProbeConfig("p").withInitValue(500).withProbe(probe::get));
            c.withMetric(new ProbeConfig("p").withInitValue(100500).withProbe(probe::get));
        });

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").name());
        assertEquals(100500, metrics.metric("p").value());
    }
}
