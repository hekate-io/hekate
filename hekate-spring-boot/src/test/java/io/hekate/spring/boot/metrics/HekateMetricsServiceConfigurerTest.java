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

package io.hekate.spring.boot.metrics;

import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricsListener;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.MetricsUpdateEvent;
import io.hekate.metrics.ProbeConfig;
import io.hekate.metrics.internal.DefaultMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import java.util.List;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HekateMetricsServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    static class LocalMetricsTestConfig extends HekateTestConfigBase {
        private static class InnerBean {
            @NamedCounter("test.counter2")
            private CounterMetric innerCounter;

            @NamedMetric("test.counter1")
            private Metric innerMetric;
        }

        @NamedCounter("test.counter1")
        private CounterMetric counter;

        @NamedMetric("test.counter2")
        private Metric metric;

        @Bean
        public InnerBean innerBean() {
            return new InnerBean();
        }

        @Bean
        public CounterMetric counter1(MetricsService metricsService) {
            return metricsService.getCounter("test.counter1");
        }

        @Bean
        public CounterMetric counter2(MetricsService metricsService) {
            return metricsService.getCounter("test.counter2");
        }

        @Bean
        public Metric probe1(MetricsService metricsService) {
            return metricsService.getMetric("test.probe1");
        }

        @Bean
        public Metric probe2(MetricsService metricsService) {
            return metricsService.getMetric("test.probe2");
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
    static class MetricListenerTestConfig extends HekateTestConfigBase {
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

        assertNotNull(get("metricsService", MetricsService.class));

        assertNotNull(get(LocalMetricsTestConfig.class).counter);
        assertNotNull(get(LocalMetricsTestConfig.class).metric);
        assertNotNull(get(LocalMetricsTestConfig.InnerBean.class).innerCounter);
        assertNotNull(get(LocalMetricsTestConfig.InnerBean.class).innerMetric);

        assertNotNull(getNode().get(MetricsService.class).getMetric("test.counter1"));
        assertNotNull(getNode().get(MetricsService.class).getMetric("test.counter2"));

        assertNotNull(getNode().get(MetricsService.class).getCounter("test.counter1"));
        assertNotNull(getNode().get(MetricsService.class).getCounter("test.counter2"));

        assertNotNull(getNode().get(MetricsService.class).getMetric("test.probe1"));
        assertNotNull(getNode().get(MetricsService.class).getMetric("test.probe2"));

        class TestAutowire {
            @Autowired
            private MetricsService localMetricsService;

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

        List<MetricsListener> listeners = getNode().get(DefaultMetricsService.class).getListeners();

        assertTrue(listeners.stream().anyMatch(l -> l instanceof MetricListenerTestConfig.Listener));
    }
}
