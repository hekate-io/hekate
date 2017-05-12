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

package io.hekate.spring.boot.metrics.local;

import io.hekate.core.Hekate;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.MetricConfigBase;
import io.hekate.metrics.local.MetricsListener;
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
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link LocalMetricsService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link LocalMetricsServiceFactory} type and automatically {@link
 * LocalMetricsServiceFactory#setMetrics(List) registers} all {@link Bean}s of {@link MetricConfigBase} and {@link MetricsListener} types.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * LocalMetricsServiceFactory} type.
 * </p>
 *
 * <h2>Configuration properties</h2>
 * <p>
 * It is possible to configure {@link LocalMetricsServiceFactory} via application properties prefixed with {@code 'hekate.metrics.local'}
 * (for example {@link LocalMetricsServiceFactory#setRefreshInterval(long) 'hekate.metrics.local.refresh-interval'})
 * </p>
 *
 * <h2>Metrics injections</h2>
 * <p>
 * This auto-configuration provides support for injecting beans of {@link CounterMetric} and {@link Metric} type into other beans with
 * the help of {@link InjectCounter} and {@link InjectMetric} annotations.
 * </p>
 *
 * <p>
 * Please see the documentation of the following annotations for more details:
 * </p>
 * <ul>
 * <li>{@link InjectCounter} - for injection of {@link CounterMetric}s</li>
 * <li>{@link InjectMetric} - for injection of {@link Metric}s</li>
 * </ul>
 *
 * @see LocalMetricsService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(LocalMetricsServiceFactory.class)
public class HekateLocalMetricsServiceConfigurer {
    @Component
    static class NamedCounterInjector extends AnnotationInjectorBase<InjectCounter> {
        public NamedCounterInjector() {
            super(InjectCounter.class, CounterMetricBean.class);
        }

        @Override
        protected String injectedBeanName(InjectCounter annotation) {
            return CounterMetricBean.class.getName() + "-" + annotation.value();
        }

        @Override
        protected Object qualifierValue(InjectCounter annotation) {
            return annotation.value();
        }

        @Override
        protected void configure(BeanDefinitionBuilder builder, InjectCounter annotation) {
            builder.addPropertyValue("name", annotation.value());
        }
    }

    @Component
    static class NamedMetricInjector extends AnnotationInjectorBase<InjectMetric> {
        public NamedMetricInjector() {
            super(InjectMetric.class, MetricBean.class);
        }

        @Override
        protected String injectedBeanName(InjectMetric annotation) {
            return MetricBean.class.getName() + "-" + annotation.value();
        }

        @Override
        protected Object qualifierValue(InjectMetric annotation) {
            return annotation.value();
        }

        @Override
        protected void configure(BeanDefinitionBuilder builder, InjectMetric annotation) {
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
    public HekateLocalMetricsServiceConfigurer(Optional<List<MetricConfigBase<?>>> metrics, Optional<List<MetricsListener>> listeners) {
        this.metrics = metrics.orElse(null);
        this.listeners = listeners.orElse(null);
    }

    /**
     * Constructs the {@link LocalMetricsServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.metrics.local")
    public LocalMetricsServiceFactory metricsServiceFactory() {
        LocalMetricsServiceFactory factory = new LocalMetricsServiceFactory();

        factory.setMetrics(metrics);
        factory.setListeners(listeners);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link LocalMetricsService} directly into other beans instead of accessing
     * it via {@link Hekate#localMetrics()} method.
     *
     * @return Service bean.
     */
    @Bean
    public LocalMetricsServiceBean metricsService() {
        return new LocalMetricsServiceBean();
    }
}
