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
import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.LocalMetricsServiceFactory;

import static org.junit.Assert.assertSame;

public abstract class LocalMetricsServiceTestBase extends HekateNodeTestBase {
    public interface MetricsConfigurer {
        void configure(LocalMetricsServiceFactory factory);
    }

    public static final int TEST_METRICS_REFRESH_INTERVAL = 25;

    protected DefaultLocalMetricsService metrics;

    protected Hekate metricsNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        metricsNode = createMetricsNode().join();

        metrics = metricsNode.get(DefaultLocalMetricsService.class);

        assertSame(metricsNode.localMetrics(), metrics);
    }

    protected void awaitForMetric(long val, Metric metric) throws Exception {
        busyWait("metric value [value=" + val + ", metric=" + metric + ']', () ->
            val == metric.value()
        );
    }

    protected HekateTestNode createMetricsNode(MetricsConfigurer configurer) throws Exception {
        return createNode(c -> {
            LocalMetricsServiceFactory metrics = new LocalMetricsServiceFactory();

            metrics.setRefreshInterval(TEST_METRICS_REFRESH_INTERVAL);

            if (configurer != null) {
                configurer.configure(metrics);
            }

            c.withService(metrics);
        });
    }

    protected HekateTestNode createMetricsNode() throws Exception {
        return createMetricsNode(null);
    }

    protected void restart(MetricsConfigurer configurer) throws Exception {
        metricsNode.leave();

        metricsNode = createMetricsNode(configurer).join();

        metrics = metricsNode.get(DefaultLocalMetricsService.class);

        assertSame(metricsNode.localMetrics(), metrics);
    }
}
