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

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.MetricsListener;
import io.hekate.metrics.local.MetricsSnapshot;
import io.hekate.metrics.local.MetricsUpdateEvent;
import io.hekate.metrics.local.ProbeConfig;
import io.hekate.util.format.ToString;
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

public class LocalMetricsServiceTest extends HekateNodeTestBase {
    public interface MetricsConfigurer {
        void configure(LocalMetricsServiceFactory factory);
    }

    public static final int TEST_METRICS_REFRESH_INTERVAL = 25;

    private final AtomicReference<Exchanger<MetricsUpdateEvent>> asyncEventRef = new AtomicReference<>();

    private DefaultLocalMetricsService metrics;

    private Hekate node;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createMetricsNode().join();

        metrics = node.get(DefaultLocalMetricsService.class);

        assertSame(node.localMetrics(), metrics);
    }

    @Test
    public void testEmptyService() throws Exception {
        assertEquals(ToString.format(LocalMetricsService.class, metrics), metrics.toString());

        assertTrue(metrics.listeners().isEmpty());
        assertEquals(TEST_METRICS_REFRESH_INTERVAL, metrics.refreshInterval());

        assertNotNull(metrics.allMetrics());
        assertTrue(metrics.allMetrics().toString().replaceAll("],", System.lineSeparator()),
            metrics.allMetrics().keySet().stream()
                .noneMatch(m -> !m.startsWith("hekate.") && !m.startsWith("jvm.") && !m.startsWith("network."))
        );
    }

    @Test
    public void testUpdateMetricsAfterNodeTermination() throws Exception {
        node.leave();

        // Should not throw any errors.
        metrics.updateMetrics();
    }

    @Test
    public void testUnknownMetrics() throws Exception {
        assertNull(metrics.metric("c"));
        assertNotNull(metrics.allMetrics());
        assertFalse(metrics.allMetrics().containsKey("c"));
        assertEquals(0, metrics.get("c"));
        assertEquals(10, metrics.get("c", 10));
    }

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
    public void testCounterDefaultValue() throws Exception {
        metrics.register(new CounterConfig("c"));

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").name());
        assertEquals(0, metrics.metric("c").value());
    }

    @Test
    public void testCounterAutoRegister() throws Exception {
        CounterMetric counter = metrics.counter("c");

        assertNotNull(counter);

        counter.increment();

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertEquals("c", metrics.metric("c").name());
        assertEquals(1, metrics.metric("c").value());
    }

    @Test
    public void testCounterTotal() throws Exception {
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
    public void testCounterIncrement() throws Exception {
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
    public void testCounterDecrement() throws Exception {
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
    public void testCounterAdd() throws Exception {
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
    public void testCounterSubtract() throws Exception {
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
            assertEquals(e.toString(), e.getMessage(), ProbeConfig.class.getSimpleName() + ": duplicated name [value=c]");
        }

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals(1000, metrics.metric("c").value());
        assertEquals(4000, metrics.metric("p").value());
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
        assertEquals("p", metrics.metric("p").name());
        assertEquals(1000, metrics.metric("p").value());

        probe.set(2000);

        for (int j = 0; j < 3; j++) {
            assertTrue(metrics.allMetrics().containsKey("c" + j));
            assertEquals("c" + j, metrics.metric("c" + j).name());
            assertEquals(0, metrics.metric("c" + j).value());

            metrics.counter("c" + j).add(1000);
        }

        awaitForMetric(1000, metrics.metric("c0"));
        awaitForMetric(1000, metrics.metric("c1"));
        awaitForMetric(1000, metrics.metric("c2"));
        awaitForMetric(2000, metrics.metric("p"));
    }

    @Test
    public void testMergeConfigurations() throws Exception {
        AtomicInteger probe = new AtomicInteger(1000);

        restart(c -> {
            c.withRefreshInterval(Long.MAX_VALUE);
            c.withMetric(new ProbeConfig("p").withInitValue(500).withProbe(probe::get));
            c.withMetric(new ProbeConfig("p").withInitValue(100500).withProbe(probe::get));
            c.withMetric(new CounterConfig("c0"));
            c.withMetric(new CounterConfig("c0").withTotalName("c0.total"));
            c.withConfigProvider(() -> Arrays.asList(
                new CounterConfig("c0").withAutoReset(true),
                new CounterConfig("c1"),
                new CounterConfig("c2"))
            );
        });

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").name());
        assertEquals(100500, metrics.metric("p").value());

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
    public void testCounterCanNotMergeTotalNames() throws Exception {
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

    @Test
    public void testPreConfiguredMetricsAfterRejoin() throws Exception {
        AtomicInteger probe = new AtomicInteger(1000);

        restart(c -> {
            c.withMetric(new ProbeConfig("p").withInitValue(1000).withProbe(probe::get));
            c.withMetric(new CounterConfig("c0"));
            c.withConfigProvider(() -> Arrays.asList(new CounterConfig("c1"), new CounterConfig("c2")));
        });

        repeat(5, i -> {
            for (int j = 0; j < 3; j++) {
                assertTrue(metrics.allMetrics().containsKey("c" + j));
                assertEquals("c" + j, metrics.metric("c" + j).name());
                assertEquals(0, metrics.metric("c" + j).value());

                metrics.counter("c" + j).add(10);

                assertEquals(10, metrics.metric("c" + j).value());
            }

            node.leave();

            node.join();
        });
    }

    @Test
    public void testMetricsAfterRejoin() throws Exception {
        repeat(5, i -> {
            assertNull(metrics.metric("c"));

            metrics.register(new CounterConfig("c"));

            assertTrue(metrics.allMetrics().containsKey("c"));
            assertEquals("c", metrics.metric("c").name());
            assertEquals(0, metrics.metric("c").value());

            node.leave();

            node.join();
        });
    }

    @Test
    public void testPreConfiguredListeners() throws Exception {
        AtomicLong probe = new AtomicLong(100);

        MetricsListener listener = this::putAsyncEvent;

        restart(c -> {
            c.withMetric(new CounterConfig("c"));
            c.withMetric(new ProbeConfig("p").withInitValue(100).withProbe(probe::get));
            c.withListener(listener);
        });

        assertTrue(metrics.listeners().contains(listener));

        metrics.counter("c").add(1000);

        MetricsUpdateEvent event = getAsyncEvent();

        Map<String, Metric> snapshot = event.allMetrics();

        assertNotNull(snapshot);

        assertEquals(1000, snapshot.get("c").value());
        assertEquals(100, snapshot.get("p").value());

        assertSame(snapshot.get("c"), event.metric("c"));
        assertSame(snapshot.get("p"), event.metric("p"));

        metrics.counter("c").add(1000);
        probe.set(200);

        snapshot = getAsyncEvent().allMetrics();

        assertEquals(2000, snapshot.get("c").value());
        assertEquals(200, snapshot.get("p").value());

        node.leave();

        probe.set(100);

        node.join();

        assertTrue(metrics.listeners().contains(listener));

        snapshot = getAsyncEvent().allMetrics();

        assertEquals(0, snapshot.get("c").value());
        assertEquals(100, snapshot.get("p").value());
    }

    @Test
    public void testDynamicListeners() throws Exception {
        AtomicLong probe = new AtomicLong(100);

        metrics.register(new CounterConfig("c"));
        metrics.register(new ProbeConfig("p").withInitValue(100).withProbe(probe::get));

        metrics.counter("c").add(1000);

        MetricsListener listener = this::putAsyncEvent;

        metrics.addListener(listener);

        assertTrue(metrics.listeners().contains(listener));

        MetricsUpdateEvent event1 = getAsyncEvent();

        Map<String, Metric> snapshot = event1.allMetrics();

        assertNotNull(snapshot);

        assertEquals(1000, snapshot.get("c").value());
        assertEquals(100, snapshot.get("p").value());

        metrics.counter("c").add(1000);
        probe.set(200);

        MetricsUpdateEvent event2 = getAsyncEvent();

        assertTrue(event1.tick() < event2.tick());

        snapshot = event2.allMetrics();

        assertEquals(2000, snapshot.get("c").value());
        assertEquals(200, snapshot.get("p").value());

        metrics.removeListener(listener);

        assertFalse(metrics.listeners().contains(listener));

        probe.set(100);
        metrics.counter("c").add(1000);

        expect(TimeoutException.class, () ->
            getAsyncEvent(TEST_METRICS_REFRESH_INTERVAL * 2, TimeUnit.MILLISECONDS)
        );
    }

    @Test
    public void testDynamicListenerRemovedOnLeave() throws Exception {
        MetricsListener listener = event -> {
            // No-op.
        };

        metrics.addListener(listener);

        assertTrue(metrics.listeners().contains(listener));

        node.leave();

        node.join();

        assertFalse(metrics.listeners().contains(listener));
    }

    @Test
    public void testSnapshot() throws Exception {
        MetricsSnapshot snapshot = metrics.snapshot();

        for (int i = 0; i < 5; i++) {
            MetricsSnapshot previous = snapshot;

            busyWait("snapshot update", () ->
                metrics.snapshot().tick() > previous.tick()
            );

            snapshot = metrics.snapshot();
        }
    }

    @Test
    public void testErrorInListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        metrics.addListener(event -> {
            latch.countDown();

            if (latch.getCount() > 0) {
                throw TEST_ERROR;
            }
        });

        // If we couldn't await for this latch then it means that error killed the periodic metrics updater.
        await(latch);
    }

    protected void awaitForMetric(long val, Metric metric) throws Exception {
        busyWait("metric value [value=" + val + ", metric=" + metric + ']', () ->
            val == metric.value()
        );
    }

    private void putAsyncEvent(MetricsUpdateEvent event) {
        try {
            Exchanger<MetricsUpdateEvent> async = asyncEventRef.getAndSet(null);

            if (async != null) {
                async.exchange(event);
            }
        } catch (InterruptedException e) {
            // No-op.
        }
    }

    private MetricsUpdateEvent getAsyncEvent() throws Exception {
        return getAsyncEvent(3, TimeUnit.SECONDS);
    }

    private MetricsUpdateEvent getAsyncEvent(long timeout, TimeUnit unit) throws Exception {
        return asyncEventRef.updateAndGet(old -> new Exchanger<>()).exchange(null, timeout, unit);
    }

    private void restart(MetricsConfigurer configurer) throws Exception {
        node.leave();

        node = createMetricsNode(configurer).join();

        metrics = node.get(DefaultLocalMetricsService.class);

        assertSame(node.localMetrics(), metrics);
    }

    private HekateTestNode createMetricsNode() throws Exception {
        return createMetricsNode(null);
    }

    private HekateTestNode createMetricsNode(MetricsConfigurer configurer) throws Exception {
        return createNode(c -> {
            LocalMetricsServiceFactory metrics = new LocalMetricsServiceFactory();

            metrics.setRefreshInterval(TEST_METRICS_REFRESH_INTERVAL);

            if (configurer != null) {
                configurer.configure(metrics);
            }

            c.withService(metrics);
        });
    }
}
