/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.metrics.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricsListener;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.MetricsServiceFactory;
import io.hekate.metrics.MetricsUpdateEvent;
import io.hekate.metrics.ProbeConfig;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class MetricsServiceTest extends HekateNodeTestBase {
    public interface MetricsConfigurer {
        void configure(MetricsServiceFactory factory);
    }

    public static final int TEST_METRICS_REFRESH_INTERVAL = 25;

    private MetricsService metrics;

    private Hekate node;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createMetricsNode().join();

        metrics = node.get(MetricsService.class);
    }

    @Test
    public void testProbeInitValue() throws Exception {
        metrics.register(new ProbeConfig("p").withInitValue(1000).withProbe(() -> 0));

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").getName());
        assertEquals(1000, metrics.metric("p").getValue());
        assertEquals(1000, metrics.get("p"));
        assertEquals(1000, metrics.get("p", 10));
    }

    @Test
    public void testUnknownMetric() throws Exception {
        assertNull(metrics.metric("c"));
        assertNotNull(metrics.allMetrics());
        assertFalse(metrics.allMetrics().containsKey("c"));
        assertEquals(0, metrics.get("c"));
        assertEquals(10, metrics.get("c", 10));
    }

    @Test
    public void testProbeDefaultValue() throws Exception {
        metrics.register(new ProbeConfig("p").withProbe(() -> 1000));

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").getName());
        assertEquals(0, metrics.metric("p").getValue());
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

        assertEquals(2, metrics.metric("p1").getValue());
    }

    @Test
    public void testCounterInitValue() throws Exception {
        metrics.register(new CounterConfig("c"));

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").getName());
        assertEquals(0, metrics.metric("c").getValue());
    }

    @Test
    public void testCounterDefaultValue() throws Exception {
        metrics.register(new CounterConfig("c"));

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").getName());
        assertEquals(0, metrics.metric("c").getValue());
    }

    @Test
    public void testCounterTotal() throws Exception {
        CounterMetric counter = metrics.register(new CounterConfig("c").withTotalName("ct").withAutoReset(true));
        Metric total = metrics.metric("ct");

        assertEquals("ct", total.getName());
        assertEquals(0, total.getValue());

        assertNotNull(counter);
        assertNotNull(total);

        for (int i = 0; i < 3; i++) {
            counter.increment();

            awaitForMetric(0, counter);

            assertEquals(i + 1, total.getValue());
        }
    }

    @Test
    public void testCounterIncrement() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        for (int i = 0; i < 10; i++) {
            assertEquals(i, metric.getValue());
            assertEquals(i, c.getValue());

            c.increment();

            assertEquals(i + 1, metric.getValue());
            assertEquals(i + 1, c.getValue());
        }
    }

    @Test
    public void testCounterDecrement() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        c.add(9);

        Metric metric = metrics.metric("c");

        for (int i = 9; i >= 0; i--) {
            assertEquals(i, metric.getValue());
            assertEquals(i, c.getValue());

            c.decrement();

            assertEquals(i - 1, metric.getValue());
            assertEquals(i - 1, c.getValue());
        }
    }

    @Test
    public void testCounterAdd() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        c.add(1);

        assertEquals(1, metric.getValue());
        assertEquals(1, c.getValue());

        c.add(2);

        assertEquals(3, metric.getValue());
        assertEquals(3, c.getValue());

        c.add(5);

        assertEquals(8, metric.getValue());
        assertEquals(8, c.getValue());
    }

    @Test
    public void testCounterSubtract() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        Metric metric = metrics.metric("c");

        assertEquals(0, metric.getValue());
        assertEquals(0, c.getValue());

        c.subtract(2);

        assertEquals(-2, metric.getValue());
        assertEquals(-2, c.getValue());

        c.subtract(5);

        assertEquals(-7, metric.getValue());
        assertEquals(-7, c.getValue());
    }

    @Test
    public void testDuplicatedMetricNames() throws Exception {
        CounterMetric c1 = metrics.register(new CounterConfig("c"));

        assertNotNull(c1);

        c1.add(1000);

        CounterMetric c2 = metrics.register(new CounterConfig("c"));

        assertSame(c1, c2);

        metrics.register(new ProbeConfig("p").withInitValue(4000).withProbe(() -> 4000));

        try {
            metrics.register(new CounterConfig("p"));

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertTrue(e.getMessage().contains("duplicated metric name"));
        }

        try {
            metrics.register(new CounterConfig("c1").withTotalName("p"));

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertTrue(e.getMessage().contains("duplicated metric name"));
        }

        try {
            metrics.register(new ProbeConfig("c").withInitValue(6000).withProbe(() -> 0));

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(e.toString(), e.getMessage(), CounterConfig.class.getSimpleName() + ": duplicated name [value=c]");
        }

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals(1000, metrics.metric("c").getValue());
        assertEquals(4000, metrics.metric("p").getValue());
    }

    @Test
    public void testPreConfiguredMetrics() throws Exception {
        AtomicInteger probe = new AtomicInteger(1000);

        restart(c -> {
            c.withMetric(new ProbeConfig("p").withInitValue(1000).withProbe(probe::get));
            c.withMetric(new CounterConfig("c0"));
            c.withConfigProvider(() -> Arrays.asList(new CounterConfig("c1"), new CounterConfig("c2")));
        });

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").getName());
        assertEquals(1000, metrics.metric("p").getValue());

        probe.set(2000);

        for (int j = 0; j < 3; j++) {
            assertTrue(metrics.allMetrics().containsKey("c" + j));
            assertEquals("c" + j, metrics.metric("c" + j).getName());
            assertEquals(0, metrics.metric("c" + j).getValue());

            metrics.getCounter("c" + j).add(1000);
        }

        awaitForMetric(1000, metrics.metric("c0"));
        awaitForMetric(1000, metrics.metric("c1"));
        awaitForMetric(1000, metrics.metric("c2"));
        awaitForMetric(2000, metrics.metric("p"));
    }

    @Test
    public void testPreConfiguredMetricsAfterRejoin() throws Exception {
        node.leave();

        AtomicInteger probe = new AtomicInteger(1000);

        node = createMetricsNode(c -> {
            c.withMetric(new ProbeConfig("p").withInitValue(1000).withProbe(probe::get));
            c.withMetric(new CounterConfig("c0"));
            c.withConfigProvider(() -> Arrays.asList(new CounterConfig("c1"), new CounterConfig("c2")));
        }).join();

        metrics = node.get(MetricsService.class);

        repeat(5, i -> {
            for (int j = 0; j < 3; j++) {
                assertTrue(metrics.allMetrics().containsKey("c" + j));
                assertEquals("c" + j, metrics.metric("c" + j).getName());
                assertEquals(0, metrics.metric("c" + j).getValue());

                metrics.getCounter("c" + j).add(10);

                assertEquals(10, metrics.metric("c" + j).getValue());
            }

            node.leave();

            node.join();

            metrics = node.get(MetricsService.class);
        });
    }

    @Test
    public void testMetricsAfterRejoin() throws Exception {
        repeat(5, i -> {
            assertNull(metrics.metric("c"));

            metrics.register(new CounterConfig("c"));

            assertTrue(metrics.allMetrics().containsKey("c"));
            assertEquals("c", metrics.metric("c").getName());
            assertEquals(0, metrics.metric("c").getValue());

            node.leave();

            node.join();

            metrics = node.get(MetricsService.class);
        });
    }

    @Test
    public void testPreConfiguredListeners() throws Exception {
        AtomicLong probe = new AtomicLong(100);

        AtomicReference<Exchanger<MetricsUpdateEvent>> asyncRef = new AtomicReference<>(new Exchanger<>());

        restart(c -> {
            c.withMetric(new CounterConfig("c"));
            c.withMetric(new ProbeConfig("p").withInitValue(100).withProbe(probe::get));
            c.withListener(event -> {
                try {
                    Exchanger<MetricsUpdateEvent> async = asyncRef.get();

                    if (async != null) {
                        if (async.exchange(event) != null) {
                            asyncRef.set(null);
                        }
                    }
                } catch (InterruptedException e) {
                    // No-op.
                }
            });
        });

        metrics.getCounter("c").add(1000);

        MetricsUpdateEvent event = asyncRef.get().exchange(null);

        Map<String, Metric> snapshot = event.allMetrics();

        assertNotNull(snapshot);

        assertEquals(1000, snapshot.get("c").getValue());
        assertEquals(100, snapshot.get("p").getValue());

        assertSame(snapshot.get("c"), event.metric("c"));
        assertSame(snapshot.get("p"), event.metric("p"));

        this.metrics.getCounter("c").add(1000);
        probe.set(200);

        snapshot = asyncRef.get().exchange(null).allMetrics();

        assertEquals(2000, snapshot.get("c").getValue());
        assertEquals(200, snapshot.get("p").getValue());

        // Unblock listener (exchange with a non-null object).
        asyncRef.get().exchange(mock(MetricsUpdateEvent.class));

        node.leave();

        probe.set(100);

        asyncRef.set(new Exchanger<>());

        node.join();

        snapshot = asyncRef.get().exchange(null).allMetrics();

        assertEquals(0, snapshot.get("c").getValue());
        assertEquals(100, snapshot.get("p").getValue());
    }

    @Test
    public void testDynamicallyConfiguredListeners() throws Exception {
        AtomicLong probe = new AtomicLong(100);

        metrics.register(new CounterConfig("c"));
        metrics.register(new ProbeConfig("p").withInitValue(100).withProbe(probe::get));

        metrics.getCounter("c").add(1000);

        AtomicReference<Exchanger<MetricsUpdateEvent>> asyncRef = new AtomicReference<>(new Exchanger<>());

        MetricsListener listener = event -> {
            try {
                Exchanger<MetricsUpdateEvent> async = asyncRef.get();

                if (async != null) {
                    if (async.exchange(event) != null) {
                        asyncRef.set(null);
                    }
                }
            } catch (InterruptedException e) {
                // No-op.
            }
        };

        metrics.addListener(listener);

        MetricsUpdateEvent event1 = asyncRef.get().exchange(null);

        Map<String, Metric> snapshot = event1.allMetrics();

        assertNotNull(snapshot);

        assertEquals(1000, snapshot.get("c").getValue());
        assertEquals(100, snapshot.get("p").getValue());

        metrics.getCounter("c").add(1000);
        probe.set(200);

        MetricsUpdateEvent event2 = asyncRef.get().exchange(null);

        assertTrue(event1.getTick() < event2.getTick());

        snapshot = event2.allMetrics();

        assertEquals(2000, snapshot.get("c").getValue());
        assertEquals(200, snapshot.get("p").getValue());

        metrics.removeListener(listener);

        probe.set(100);
        metrics.getCounter("c").add(1000);

        expect(TimeoutException.class, () ->
            asyncRef.get().exchange(null, TEST_METRICS_REFRESH_INTERVAL * 2, TimeUnit.MILLISECONDS)
        );

        // Make sure that listener is not blocked.
        try {
            asyncRef.get().exchange(mock(MetricsUpdateEvent.class), TEST_METRICS_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Ignore.
        }
    }

    protected void awaitForMetric(long val, Metric metric) throws Exception {
        busyWait("metric value [value=" + val + ", metric=" + metric + ']', () ->
            val == metric.getValue()
        );
    }

    private void restart(MetricsConfigurer configurer) throws Exception {
        node.leave();

        node = createMetricsNode(configurer).join();

        metrics = node.get(MetricsService.class);
    }

    private HekateTestNode createMetricsNode() throws Exception {
        return createMetricsNode(null);
    }

    private HekateTestNode createMetricsNode(MetricsConfigurer configurer) throws Exception {
        return createNode(c -> {
            MetricsServiceFactory metrics = new MetricsServiceFactory();

            metrics.setRefreshInterval(TEST_METRICS_REFRESH_INTERVAL);

            if (configurer != null) {
                configurer.configure(metrics);
            }

            c.withService(metrics);
        });
    }
}
