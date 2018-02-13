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
import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.local.TimeSpan;
import io.hekate.metrics.local.TimerConfig;
import io.hekate.metrics.local.TimerMetric;
import io.hekate.util.time.SystemTimeSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LocalMetricsServiceTimerTest extends LocalMetricsServiceTestBase {
    private static class SystemTimeMock implements SystemTimeSupplier {
        private final AtomicLong time = new AtomicLong();

        @Override
        public long nanoTime() {
            return time.get();
        }

        public void set(long time) {
            this.time.set(time);
        }
    }

    private final SystemTimeMock time = new SystemTimeMock();

    @Test
    public void testAutoRegister() throws Exception {
        TimerMetric timer = metrics.timer("t");

        assertNotNull(timer);

        assertFalse(timer.hasRate());
        assertSame(TimeUnit.NANOSECONDS, timer.timeUnit());
        assertTrue(metrics.allMetrics().containsKey("t"));
        assertEquals("t", metrics.metric("t").name());
    }

    @Test
    public void testMergeConfigurations() throws Exception {
        restart(c -> {
            c.withRefreshInterval(Long.MAX_VALUE);
            c.withMetric(new TimerConfig("t0"));
            c.withMetric(new TimerConfig("t0").withRateName("t0.rate"));
            c.withConfigProvider(() -> singletonList(
                new TimerConfig("t0")
            ));
        });

        assertTrue(metrics.allMetrics().containsKey("t0"));

        TimerMetric t0 = metrics.timer("t0");

        assertSame(t0, metrics.allMetrics().get("t0"));

        assertTrue(t0.hasRate());
        assertEquals("t0", t0.name());
        assertEquals("t0.rate", t0.rate().name());
    }

    @Test
    public void testCanNotMergeRateNames() throws Exception {
        try {
            restart(c -> {
                c.withMetric(new TimerConfig("t").withRateName("t.rate1"));
                c.withMetric(new TimerConfig("t").withRateName("t.rate2"));
            });
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.getCause() instanceof HekateConfigurationException);
            assertEquals("TimerConfig: can't merge configurations of a timer metric with different 'rate' names "
                + "[timer=t, rate-name-1=t.rate1, rate-name-2=t.rate2]", e.getCause().getMessage());
        }
    }

    @Test
    public void testCanNotMergeTimeUnits() throws Exception {
        try {
            restart(c -> {
                c.withMetric(new TimerConfig("t").withTimeUnit(TimeUnit.SECONDS));
                c.withMetric(new TimerConfig("t").withTimeUnit(TimeUnit.MINUTES));
            });
        } catch (HekateFutureException e) {
            assertTrue(getStacktrace(e), e.getCause() instanceof HekateConfigurationException);
            assertEquals("TimerConfig: can't merge configurations of a timer metric with different time units "
                + "[timer=t, unit-1=SECONDS, unit-2=MINUTES]", e.getCause().getMessage());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        metrics.register(new TimerConfig("t0"));

        TimerMetric t1 = metrics.register(new TimerConfig("t1"));
        TimerMetric t2 = metrics.register(new TimerConfig("t2"));
        TimerMetric t3 = metrics.register(new TimerConfig("t3").withRateName("t3.rate"));

        repeat(5, i -> {
            assertTrue(metrics.allMetrics().containsKey("t0"));
            assertTrue(metrics.allMetrics().containsKey("t1"));
            assertTrue(metrics.allMetrics().containsKey("t2"));
            assertTrue(metrics.allMetrics().containsKey("t3"));
            assertTrue(metrics.allMetrics().containsKey("t3.rate"));

            assertEquals("t0", metrics.metric("t0").name());
            assertEquals("t1", metrics.metric("t1").name());
            assertEquals("t2", metrics.metric("t2").name());
            assertEquals("t3", metrics.metric("t3").name());

            assertEquals(0, metrics.metric("t0").value());
            assertEquals(0, metrics.metric("t1").value());
            assertEquals(0, metrics.metric("t2").value());
            assertEquals(0, metrics.metric("t3").value());
            assertEquals(0, metrics.metric("t3.rate").value());

            int updates = i + 1;
            int startTime = 10;
            int duration = 100;

            repeat(updates, j -> {
                // Fixed time as Callable.
                time.set(startTime);

                assertEquals(j, (int)t1.measure(() -> {
                    time.set(startTime + duration);

                    return j;
                }));

                // Variable time as Closable.
                time.set(10 * (j + 1));

                try (TimeSpan ignore = t2.start()) {
                    this.time.set((j + 1) * (startTime + duration));
                }

                // With rate as Runnable.
                time.set(startTime);

                t3.measure(() ->
                    time.set(startTime + duration)
                );
            });

            metrics.updateMetrics();

            assertEquals(0, metrics.snapshot().get("t0"));
            assertEquals(duration, metrics.snapshot().get("t1"));
            assertEquals(IntStream.range(1, updates + 1).map(it -> it * duration).sum() / updates, metrics.snapshot().get("t2"));
            assertEquals(duration, metrics.snapshot().get("t3"));
            assertEquals(updates, metrics.snapshot().get("t3.rate"));
        });
    }

    @Override
    protected HekateTestNode createMetricsNode() throws Exception {
        return createMetricsNode(metrics -> {
            metrics.withRefreshInterval(Long.MAX_VALUE);
            metrics.withSystemTime(time);
        });
    }
}
