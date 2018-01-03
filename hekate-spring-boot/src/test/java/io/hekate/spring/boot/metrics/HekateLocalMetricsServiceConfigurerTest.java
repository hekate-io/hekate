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

package io.hekate.spring.boot.metrics;

import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.MetricsListener;
import io.hekate.metrics.local.MetricsUpdateEvent;
import io.hekate.metrics.local.ProbeConfig;
import io.hekate.metrics.local.internal.DefaultLocalMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.spring.boot.metrics.local.InjectCounter;
import io.hekate.spring.boot.metrics.local.InjectMetric;
import java.util.List;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HekateLocalMetricsServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class LocalMetricsTestConfig extends HekateTestConfigBase {
        private static class InnerBean {
            @InjectCounter("test.counter2")
            private CounterMetric counter;

            @InjectCounter("test.non-configured.counter2")
            private CounterMetric nonConfiguredCounter;

            @InjectMetric("test.counter1")
            private Metric metric;
        }

        @InjectCounter("test.counter1")
        private CounterMetric counter;

        @InjectMetric("test.counter2")
        private Metric metric;

        @InjectCounter("test.non-configured.counter1")
        private CounterMetric nonConfiguredCounter;

        @Bean
        public InnerBean innerBean() {
            return new InnerBean();
        }

        @Bean
        public CounterMetric counter1(LocalMetricsService metricsService) {
            return metricsService.counter("test.counter1");
        }

        @Bean
        public CounterMetric counter2(LocalMetricsService metricsService) {
            return metricsService.counter("test.counter2");
        }

        @Bean
        public Metric probe1(LocalMetricsService metricsService) {
            return metricsService.metric("test.probe1");
        }

        @Bean
        public Metric probe2(LocalMetricsService metricsService) {
            return metricsService.metric("test.probe2");
        }

        @Bean
        public CounterConfig counter1Config() {
            return new CounterConfig().withName("test.counter1");
        }

        @Bean
        public CounterConfig counter2Config() {
            return new CounterConfig().withName("test.counter2");
        }

        @Bean
        public ProbeConfig probe1Config() {
            return new ProbeConfig().withName("test.probe1").withProbe(() -> 0);
        }

        @Bean
        public ProbeConfig probe2Config() {
            return new ProbeConfig().withName("test.probe2").withProbe(() -> 0);
        }
    }

    @EnableAutoConfiguration
    public static class MetricListenerTestConfig extends HekateTestConfigBase {
        private static class Listener implements MetricsListener {
            @Override
            public void onUpdate(MetricsUpdateEvent event) {
                // No-op.
            }
        }

        @Bean
        public MetricsListener listener() {
            return new Listener();
        }
    }

    @Test
    public void testMetrics() {
        registerAndRefresh(LocalMetricsTestConfig.class);

        assertNotNull(get("metricsService", LocalMetricsService.class));

        assertNotNull(get(LocalMetricsTestConfig.class).counter);
        assertNotNull(get(LocalMetricsTestConfig.class).nonConfiguredCounter);
        assertNotNull(get(LocalMetricsTestConfig.class).metric);

        assertNotNull(get(LocalMetricsTestConfig.InnerBean.class).counter);
        assertNotNull(get(LocalMetricsTestConfig.InnerBean.class).nonConfiguredCounter);
        assertNotNull(get(LocalMetricsTestConfig.InnerBean.class).metric);

        assertNotNull(getNode().localMetrics().metric("test.counter1"));
        assertNotNull(getNode().localMetrics().metric("test.counter2"));
        assertNotNull(getNode().localMetrics().metric("test.non-configured.counter1"));
        assertNotNull(getNode().localMetrics().metric("test.non-configured.counter2"));

        assertNotNull(getNode().localMetrics().counter("test.counter1"));
        assertNotNull(getNode().localMetrics().counter("test.counter2"));
        assertNotNull(getNode().localMetrics().counter("test.non-configured.counter1"));
        assertNotNull(getNode().localMetrics().counter("test.non-configured.counter2"));

        assertNotNull(getNode().localMetrics().metric("test.probe1"));
        assertNotNull(getNode().localMetrics().metric("test.probe2"));

        class TestAutowire {
            @Autowired
            private LocalMetricsService localMetricsService;

            @Autowired
            @Qualifier("counter1")
            private CounterMetric counter1;

            @Autowired
            @Qualifier("counter1")
            private CounterMetric counter2;

            @Autowired
            @Qualifier("probe1")
            private Metric probe1;

            @Autowired
            @Qualifier("probe2")
            private Metric probe2;
        }

        assertNotNull(autowire(new TestAutowire()).localMetricsService);
        assertNotNull(autowire(new TestAutowire()).counter1);
        assertNotNull(autowire(new TestAutowire()).counter2);
        assertNotNull(autowire(new TestAutowire()).probe1);
        assertNotNull(autowire(new TestAutowire()).probe2);
    }

    @Test
    public void testListener() {
        registerAndRefresh(MetricListenerTestConfig.class);

        List<MetricsListener> listeners = getNode().get(DefaultLocalMetricsService.class).listeners();

        assertTrue(listeners.stream().anyMatch(l -> l instanceof MetricListenerTestConfig.Listener));
    }
}
