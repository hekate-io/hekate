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
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.MetricsListener;
import io.hekate.metrics.local.MetricsSnapshot;
import io.hekate.metrics.local.MetricsUpdateEvent;
import io.hekate.metrics.local.ProbeConfig;
import io.hekate.metrics.local.TimerConfig;
import io.hekate.metrics.local.TimerMetric;
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

public class LocalMetricsServiceTest extends LocalMetricsServiceTestBase {
    private final AtomicReference<Exchanger<MetricsUpdateEvent>> asyncEventRef = new AtomicReference<>();

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
        metricsNode.leave();

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
    public void testDuplicatedMetricNames() throws Exception {
        CounterMetric c = metrics.register(new CounterConfig("c"));

        c.add(1000);

        assertSame(c, metrics.register(new CounterConfig("c")));

        metrics.register(new ProbeConfig("p").withInitValue(4000).withProbe(() -> 4000));

        TimerMetric t = metrics.register(new TimerConfig("t"));

        assertSame(t, metrics.register(new TimerConfig("t")));

        expectDuplicatedNameError(() -> metrics.register(new CounterConfig("p")));
        expectDuplicatedNameError(() -> metrics.register(new CounterConfig("t")));
        expectDuplicatedNameError(() -> metrics.register(new CounterConfig("c-other").withTotalName("p")));
        expectDuplicatedNameError(() -> metrics.register(new CounterConfig("c-other").withTotalName("t")));
        expectDuplicatedNameError(() -> metrics.register(new ProbeConfig("c").withInitValue(6000).withProbe(() -> 0)));
        expectDuplicatedNameError(() -> metrics.register(new ProbeConfig("t").withInitValue(6000).withProbe(() -> 0)));
        expectDuplicatedNameError(() -> metrics.register(new TimerConfig("c")));
        expectDuplicatedNameError(() -> metrics.register(new TimerConfig("p")));
        expectDuplicatedNameError(() -> metrics.register(new TimerConfig("t-other").withRateName("c")));
        expectDuplicatedNameError(() -> metrics.register(new TimerConfig("t-other").withRateName("p")));

        assertTrue(metrics.allMetrics().containsKey("c"));
        assertTrue(metrics.allMetrics().containsKey("p"));
        assertTrue(metrics.allMetrics().containsKey("t"));

        assertFalse(metrics.allMetrics().containsKey("c-other"));
        assertFalse(metrics.allMetrics().containsKey("t-other"));

        assertEquals(1000, metrics.metric("c").value());
        assertEquals(4000, metrics.metric("p").value());
    }

    @Test
    public void testPreConfiguredMetrics() throws Exception {
        AtomicInteger probe = new AtomicInteger(1000);

        restart(c -> {
            c.withMetric(new ProbeConfig("p").withInitValue(1000).withProbe(probe::get));
            c.withMetric(new TimerConfig("t"));
            c.withMetric(new CounterConfig("c0"));
            c.withConfigProvider(() -> Arrays.asList(new CounterConfig("c1"), new CounterConfig("c2")));
        });

        assertTrue(metrics.allMetrics().containsKey("p"));
        assertEquals("p", metrics.metric("p").name());
        assertEquals(1000, metrics.metric("p").value());

        assertTrue(metrics.allMetrics().containsKey("t"));
        assertEquals("t", metrics.metric("t").name());

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

            metricsNode.leave();

            metricsNode.join();
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

            metricsNode.leave();

            metricsNode.join();
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

        metricsNode.leave();

        probe.set(100);

        metricsNode.join();

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

        metricsNode.leave();

        metricsNode.join();

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

    private void expectDuplicatedNameError(TestTask task) {
        HekateConfigurationException err = expect(HekateConfigurationException.class, task);

        assertTrue(err.getMessage(), err.getMessage().contains("duplicated metric name"));
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
}
