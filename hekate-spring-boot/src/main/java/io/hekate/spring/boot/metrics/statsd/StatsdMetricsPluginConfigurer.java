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

package io.hekate.spring.boot.metrics.statsd;

import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.MetricFilterGroup;
import io.hekate.metrics.MetricNameFilter;
import io.hekate.metrics.MetricRegexFilter;
import io.hekate.metrics.influxdb.InfluxDbMetricsConfig;
import io.hekate.metrics.statsd.StatsdMetricsConfig;
import io.hekate.metrics.statsd.StatsdMetricsPlugin;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Auto-configuration for {@link StatsdMetricsPlugin}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * StatsD integration is provided by the 'hekate-metrics-statsd' module and can be imported into the project dependency management
 * system as in the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#maven">Maven</a></li>
 * <li><a href="#gradle">Gradle</a></li>
 * <li><a href="#ivy">Ivy</a></li>
 * </ul>
 * <div id="maven">
 * <pre>{@code
 * <dependency>
 *   <groupId>io.hekate</groupId>
 *   <artifactId>hekate-metrics-statsd</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-metrics-statsd', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-metrics-statsd" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.metrics.statsd.enable'} property to
 * {@code true} in the application's configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link StatsdMetricsPlugin} instance:
 * </p>
 * <ul>
 * <li>{@link StatsdMetricsConfig#setHost(String) 'hekate.metrics.statsd.host'}</li>
 * <li>{@link StatsdMetricsConfig#setPort(int) 'hekate.metrics.statsd.port'}</li>
 * <li>{@link StatsdMetricsConfig#setBatchSize(int) 'hekate.metrics.statsd.batch-size'}</li>
 * <li>{@link StatsdMetricsConfig#setMaxQueueSize(int) 'hekate.metrics.statsd.max-queue-size'}</li>
 * <li>'hekate.metrics.statsd.regex-filters' - list of regular expressions to filter metrics that should be published to StatsD</li>
 * <li>'hekate.metrics.statsd.name-filters' - list of metric names that should be published to StatsD</li>
 * </ul>
 *
 * @see StatsdMetricsPlugin
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnClass(StatsdMetricsPlugin.class)
@ConditionalOnProperty(value = "hekate.metrics.statsd.enable", havingValue = "true")
public class StatsdMetricsPluginConfigurer {
    @Component
    @ConfigurationProperties("hekate.metrics.statsd")
    static class StatsdFilterProperties {
        private List<String> regexFilters;

        private List<String> nameFilters;

        public List<String> getRegexFilters() {
            return regexFilters;
        }

        public void setRegexFilters(List<String> regexFilters) {
            this.regexFilters = regexFilters;
        }

        public List<String> getNameFilters() {
            return nameFilters;
        }

        public void setNameFilters(List<String> nameFilters) {
            this.nameFilters = nameFilters;
        }
    }

    /**
     * Filter group for {@link #statsdMetricsConfig(MetricFilterGroup)}.
     *
     * @param filterProps Filter properties.
     *
     * @return Filter group.
     *
     * @see InfluxDbMetricsConfig#setFilter(MetricFilter)
     */
    @Bean
    @Qualifier("statsdMetricFilter")
    public MetricFilterGroup statsdMetricFilter(StatsdFilterProperties filterProps) {
        MetricFilterGroup group = new MetricFilterGroup();

        if (!CollectionUtils.isEmpty(filterProps.getRegexFilters())) {
            filterProps.getRegexFilters().stream()
                .filter(it -> it != null && !it.trim().isEmpty())
                .map(regex -> new MetricRegexFilter(regex.trim()))
                .forEach(group::withFilter);
        }

        if (!CollectionUtils.isEmpty(filterProps.getNameFilters())) {
            filterProps.getNameFilters().stream()
                .filter(it -> it != null && !it.trim().isEmpty())
                .map(it -> new MetricNameFilter(it.trim()))
                .forEach(group::withFilter);
        }

        return group;
    }

    /**
     * Conditionally constructs a new configuration for {@link StatsdMetricsPlugin} if application doesn't provide its own {@link Bean}
     * of {@link StatsdMetricsConfig} type.
     *
     * @param filter Metric filter (see {@link #statsdMetricFilter(StatsdFilterProperties)}).
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(StatsdMetricsConfig.class)
    @ConfigurationProperties(prefix = "hekate.metrics.statsd")
    public StatsdMetricsConfig statsdMetricsConfig(@Qualifier("statsdMetricFilter") MetricFilterGroup filter) {
        StatsdMetricsConfig cfg = new StatsdMetricsConfig();

        if (!CollectionUtils.isEmpty(filter.getFilters())) {
            cfg.setFilter(filter);
        }

        return cfg;
    }

    /**
     * Constructs new {@link StatsdMetricsPlugin}.
     *
     * @param cfg Configuration (see {@link #statsdMetricsConfig(MetricFilterGroup)}).
     *
     * @return New plugin.
     */
    @Bean
    public StatsdMetricsPlugin statsdMetricsPlugin(StatsdMetricsConfig cfg) {
        return new StatsdMetricsPlugin(cfg);
    }
}
