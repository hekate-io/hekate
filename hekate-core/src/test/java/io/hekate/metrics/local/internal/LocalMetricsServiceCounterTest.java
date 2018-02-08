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

import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateFutureException;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LocalMetricsServiceCounterTest extends LocalMetricsServiceTestBase {
    @Test
    public void testDefaultValue() throws Exception {
        metrics.register(new CounterConfig("c"));

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").name());
        assertEquals(0, metrics.metric("c").value());
    }

    @Test
    public void testAutoRegister() throws Exception {
        CounterMetric counter = metrics.counter("c");

        assertNotNull(counter);

        counter.increment();

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").name());
        assertEquals(1, metrics.metric("c").value());
    }

    @Test
    public void testTotal() throws Exception {
        CounterMetric counter = metrics.register(new CounterConfig("c").withTotalName("ct").withAutoReset(true));
        Metric total = metrics.metric("ct");

        assertNotNull(counter);
        assertNotNull(total);

        assertTrue(counter.isAutoReset());
        assertTrue(counter.hasTotal());
        assertSame(total, counter.total());
        assertEquals("ct", total.name());
        assertEquals(0, total.value());

        for (int i = 0; i < 3; i++) {
            counter.increment();

            awaitForMetric(0, counter);

            assertEquals(i + 1, total.value());
        }
    }

    @Test
    public void testIncrement() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        for (int i = 0; i < 10; i++) {
            assertEquals(i, metric.value());
            assertEquals(i, c.value());

            c.increment();

            assertEquals(i + 1, metric.value());
            assertEquals(i + 1, c.value());
        }
    }

    @Test
    public void testDecrement() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        c.add(9);

        Metric metric = metrics.metric("c");

        for (int i = 9; i >= 0; i--) {
            assertEquals(i, metric.value());
            assertEquals(i, c.value());

            c.decrement();

            assertEquals(i - 1, metric.value());
            assertEquals(i - 1, c.value());
        }
    }

    @Test
    public void testAdd() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        c.add(1);

        assertEquals(1, metric.value());
        assertEquals(1, c.value());

        c.add(2);

        assertEquals(3, metric.value());
        assertEquals(3, c.value());

        c.add(5);

        assertEquals(8, metric.value());
        assertEquals(8, c.value());
    }

    @Test
    public void testSubtract() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        assertEquals(0, metric.value());
        assertEquals(0, c.value());

        c.subtract(2);

        assertEquals(-2, metric.value());
        assertEquals(-2, c.value());

        c.subtract(5);

        assertEquals(-7, metric.value());
        assertEquals(-7, c.value());
    }

    @Test
    public void testMergeConfigurations() throws Exception {
        restart(c -> {
            c.withRefreshInterval(Long.MAX_VALUE);
            c.withMetric(new CounterConfig("c0"));
            c.withMetric(new CounterConfig("c0").withTotalName("c0.total"));
            c.withConfigProvider(() -> Arrays.asList(
                new CounterConfig("c0").withAutoReset(true),
                new CounterConfig("c1"),
                new CounterConfig("c2"))
            );
        });

        assertTrue(metrics.allMetrics().containsKey("c0"));
        assertTrue(metrics.allMetrics().containsKey("c1"));
        assertTrue(metrics.allMetrics().containsKey("c2"));

        CounterMetric c0 = metrics.counter("c0");

        assertSame(c0, metrics.allMetrics().get("c0"));

        c0.increment();

        assertEquals("c0", c0.name());
        assertEquals(1, c0.value());
        assertTrue(c0.isAutoReset());
        assertTrue(c0.hasTotal());
        assertEquals("c0.total", c0.total().name());
        assertEquals(1, c0.total().value());
    }

    @Test
    public void testCanNotMergeTotalNames() throws Exception {
        try {
            restart(c -> {
                c.withMetric(new CounterConfig("c").withTotalName("c.total1"));
                c.withMetric(new CounterConfig("c").withTotalName("c.total2"));
            });
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.getCause() instanceof HekateConfigurationException);
            assertEquals("CounterConfig: can't merge configurations of a counter metric with different 'total' names "
                + "[counter=c, total-name-1=c.total1, total-name-2=c.total2]", e.getCause().getMessage());
        }
    }
}
