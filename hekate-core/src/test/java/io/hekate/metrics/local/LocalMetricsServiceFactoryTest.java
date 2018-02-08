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

import io.hekate.HekateTestBase;
import io.hekate.core.HekateException;
import io.hekate.util.format.ToString;
import io.hekate.util.time.SystemTimeSupplier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LocalMetricsServiceFactoryTest extends HekateTestBase {
    private final LocalMetricsServiceFactory factory = new LocalMetricsServiceFactory();

    @Test
    public void testCreateService() throws HekateException {
        assertNotNull(factory.createService());
    }

    @Test
    public void testRefreshInterval() {
        assertEquals(1000, LocalMetricsServiceFactory.DEFAULT_REFRESH_INTERVAL);
        assertEquals(LocalMetricsServiceFactory.DEFAULT_REFRESH_INTERVAL, factory.getRefreshInterval());

        factory.setRefreshInterval(2000);

        assertEquals(2000, factory.getRefreshInterval());

        assertSame(factory, factory.withRefreshInterval(30000));

        assertEquals(30000, factory.getRefreshInterval());
    }

    @Test
    public void testMetrics() {
        assertNull(factory.getMetrics());

        CounterConfig c1 = new CounterConfig();
        CounterConfig c2 = new CounterConfig();
        CounterConfig c3 = new CounterConfig();

        factory.setMetrics(new ArrayList<>(Arrays.asList(c1, c2)));

        assertEquals(2, factory.getMetrics().size());
        assertTrue(factory.getMetrics().contains(c1));
        assertTrue(factory.getMetrics().contains(c2));

        assertSame(factory, factory.withMetric(c3));

        assertEquals(3, factory.getMetrics().size());
        assertTrue(factory.getMetrics().contains(c1));
        assertTrue(factory.getMetrics().contains(c2));
        assertTrue(factory.getMetrics().contains(c3));

        factory.setMetrics(null);

        assertNull(factory.getMetrics());

        factory.withMetric(c1);
        factory.withMetric(c2);
        factory.withMetric(c3);

        assertEquals(3, factory.getMetrics().size());
        assertTrue(factory.getMetrics().contains(c1));
        assertTrue(factory.getMetrics().contains(c2));
        assertTrue(factory.getMetrics().contains(c3));
    }

    @Test
    public void testMetricProviders() {
        MetricsConfigProvider p1 = Collections::emptyList;
        MetricsConfigProvider p2 = Collections::emptyList;

        assertNull(factory.getConfigProviders());

        factory.setConfigProviders(Arrays.asList(p1, p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));

        factory.setConfigProviders(null);

        assertNull(factory.getConfigProviders());

        assertSame(factory, factory.withConfigProvider(p1));

        assertEquals(1, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
    }

    @Test
    public void testListeners() {
        MetricsListener l1 = metrics -> {
            // No-op.
        };
        MetricsListener l2 = metrics -> {
            // No-op.
        };

        assertNull(factory.getListeners());

        factory.setListeners(Arrays.asList(l1, l2));

        assertEquals(2, factory.getListeners().size());
        assertTrue(factory.getListeners().contains(l1));
        assertTrue(factory.getListeners().contains(l2));

        factory.setListeners(null);

        assertNull(factory.getListeners());

        assertSame(factory, factory.withListener(l1));

        assertEquals(1, factory.getListeners().size());
        assertTrue(factory.getListeners().contains(l1));
    }

    @Test
    public void testSystemTime() {
        assertNull(factory.getSystemTime());

        SystemTimeSupplier time = () -> 0;

        factory.setSystemTime(time);

        assertSame(time, factory.getSystemTime());

        factory.setSystemTime(null);

        assertNull(factory.getSystemTime());

        assertSame(factory, factory.withSystemTime(time));

        assertSame(time, factory.getSystemTime());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
