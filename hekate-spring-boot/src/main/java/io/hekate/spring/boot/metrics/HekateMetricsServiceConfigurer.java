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

import io.hekate.core.Hekate;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricConfigBase;
import io.hekate.metrics.MetricsListener;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.MetricsServiceFactory;
import io.hekate.spring.bean.metrics.CounterMetricBean;
import io.hekate.spring.bean.metrics.LocalMetricsServiceBean;
import io.hekate.spring.bean.metrics.MetricBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.spring.boot.internal.AnnotationInjectorBase;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link MetricsService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link MetricsServiceFactory} type and automatically {@link
 * MetricsServiceFactory#setMetrics(List) registers} all {@link Bean}s of {@link MetricConfigBase} and {@link MetricsListener} types.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * MetricsServiceFactory} type.
 * </p>
 *
 * <h2>Configuration properties</h2>
 * <p>
 * It is possible to configure {@link MetricsServiceFactory} via application properties prefixed with {@code 'hekate.metrics'}
 * (for example {@link MetricsServiceFactory#setRefreshInterval(long) 'hekate.metrics.refresh-interval'})
 * </p>
 *
 * <h2>Metrics injections</h2>
 * <p>
 * This auto-configuration provides support for injecting beans of {@link CounterMetric} and {@link Metric} type into other beans with
 * the help of {@link NamedCounter} and {@link NamedMetric} annotations.
 * </p>
 *
 * <p>
 * Please see the documentation of the following annotations for more details:
 * </p>
 * <ul>
 * <li>{@link NamedCounter} - for injection of {@link CounterMetric}s</li>
 * <li>{@link NamedMetric} - for injection of {@link Metric}s</li>
 * </ul>
 *
 * @see MetricsService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(MetricsServiceFactory.class)
public class HekateMetricsServiceConfigurer {
    @Component
    static class NamedCounterInjector extends AnnotationInjectorBase<NamedCounter> {
        public NamedCounterInjector() {
            super(NamedCounter.class, CounterMetricBean.class);
        }

        @Override
        protected String getInjectedBeanName(NamedCounter annotation) {
            return CounterMetricBean.class.getName() + "-" + annotation.value();
        }

        @Override
        protected Object getQualifierValue(NamedCounter annotation) {
            return annotation.value();
        }

        @Override
        protected void configure(BeanDefinitionBuilder builder, NamedCounter annotation) {
            builder.addPropertyValue("name", annotation.value());
        }
    }

    @Component
    static class NamedMetricInjector extends AnnotationInjectorBase<NamedMetric> {
        public NamedMetricInjector() {
            super(NamedMetric.class, MetricBean.class);
        }

        @Override
        protected String getInjectedBeanName(NamedMetric annotation) {
            return MetricBean.class.getName() + "-" + annotation.value();
        }

        @Override
        protected Object getQualifierValue(NamedMetric annotation) {
            return annotation.value();
        }

        @Override
        protected void configure(BeanDefinitionBuilder builder, NamedMetric annotation) {
            builder.addPropertyValue("name", annotation.value());
        }
    }

    private final List<MetricConfigBase<?>> metrics;

    private final List<MetricsListener> listeners;

    /**
     * Constructs new instance.
     *
     * @param metrics {@link MetricConfigBase}s that were found in the application context.
     * @param listeners {@link MetricsListener}s that were found in the application context.
     */
    public HekateMetricsServiceConfigurer(Optional<List<MetricConfigBase<?>>> metrics, Optional<List<MetricsListener>> listeners) {
        this.metrics = metrics.orElse(null);
        this.listeners = listeners.orElse(null);
    }

    /**
     * Constructs the {@link MetricsServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.metrics")
    public MetricsServiceFactory metricsServiceFactory() {
        MetricsServiceFactory factory = new MetricsServiceFactory();

        factory.setMetrics(metrics);
        factory.setListeners(listeners);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link MetricsService} directly into other beans instead of accessing
     * it via {@link Hekate#metrics()} method.
     *
     * @return Service bean.
     */
    @Bean
    public LocalMetricsServiceBean metricsService() {
        return new LocalMetricsServiceBean();
    }
}
