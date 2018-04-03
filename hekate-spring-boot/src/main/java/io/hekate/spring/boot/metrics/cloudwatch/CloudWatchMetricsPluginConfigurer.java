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

package io.hekate.spring.boot.metrics.cloudwatch;

import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.MetricFilterGroup;
import io.hekate.metrics.MetricNameFilter;
import io.hekate.metrics.MetricRegexFilter;
import io.hekate.metrics.cloudwatch.CloudWatchMetaDataProvider;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsConfig;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsPlugin;
import io.hekate.metrics.influxdb.InfluxDbMetricsPlugin;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import java.util.Optional;
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
 * Auto-configuration for {@link CloudWatchMetricsConfig}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * Amazon CloudWatch integration is provided by the 'hekate-metrics-cloudwatch' module and can be imported into the project dependency
 * management system as in the example below:
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
 *   <artifactId>hekate-metrics-cloudwatch</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-metrics-cloudwatch', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-metrics-cloudwatch" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.metrics.cloudwatch.enable'} property to
 * {@code true} in the application's configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link CloudWatchMetricsPlugin} instance:
 * </p>
 * <ul>
 * <li>{@link CloudWatchMetricsConfig#setPublishInterval(int) 'hekate.metrics.cloudwatch.publish-interval'}</li>
 * <li>{@link CloudWatchMetricsConfig#setNamespace(String) 'hekate.metrics.cloudwatch.namespace'}</li>
 * <li>{@link CloudWatchMetricsConfig#setRegion(String) 'hekate.metrics.cloudwatch.region'}</li>
 * <li>{@link CloudWatchMetricsConfig#setAccessKey(String) 'hekate.metrics.cloudwatch.access-key'}</li>
 * <li>{@link CloudWatchMetricsConfig#setSecretKey(String) 'hekate.metrics.cloudwatch.secret-key'}</li>
 * <li>'hekate.metrics.cloudwatch.regex-filters' - list of regular expressions to filter metrics that should be published to CloudWatch</li>
 * <li>'hekate.metrics.cloudwatch.name-filters' - list of metric names that should be published to CloudWatch</li>
 * </ul>
 *
 * @see CloudWatchMetricsPlugin
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnClass(InfluxDbMetricsPlugin.class)
@ConditionalOnProperty(value = "hekate.metrics.cloudwatch.enable", havingValue = "true")
public class CloudWatchMetricsPluginConfigurer {
    @Component
    @ConfigurationProperties("hekate.metrics.cloudwatch")
    static class CloudWatchFilterProperties {
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
     * Filter group for {@link #cloudWatchMetricsConfig(Optional, MetricFilterGroup)}.
     *
     * @param filterProps Filter properties.
     *
     * @return Filter group.
     *
     * @see CloudWatchMetricsConfig#setFilter(MetricFilter)
     */
    @Bean
    @Qualifier("cloudWatchMetricFilter")
    public MetricFilterGroup cloudWatchMetricFilter(CloudWatchFilterProperties filterProps) {
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
     * Conditionally constructs a new configuration for {@link CloudWatchMetricsPlugin} if application doesn't provide its own {@link Bean}
     * of {@link CloudWatchMetricsConfig} type.
     *
     * @param metaDataProvider AWS meta-data provider.
     * @param filter Metric filter (see {@link #cloudWatchMetricFilter(CloudWatchFilterProperties)} ).
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(CloudWatchMetricsConfig.class)
    @ConfigurationProperties(prefix = "hekate.metrics.cloudwatch")
    public CloudWatchMetricsConfig cloudWatchMetricsConfig(
        Optional<CloudWatchMetaDataProvider> metaDataProvider,
        @Qualifier("cloudWatchMetricFilter") MetricFilterGroup filter
    ) {
        CloudWatchMetricsConfig cfg = new CloudWatchMetricsConfig();

        metaDataProvider.ifPresent(cfg::setMetaDataProvider);

        if (!CollectionUtils.isEmpty(filter.getFilters())) {
            cfg.setFilter(filter);
        }

        return cfg;
    }

    /**
     * Constructs new {@link CloudWatchMetricsPlugin}.
     *
     * @param cfg Configuration (see {@link #cloudWatchMetricsConfig(Optional, MetricFilterGroup)}.
     *
     * @return New plugin.
     */
    @Bean
    public CloudWatchMetricsPlugin cloudWatchMetricsPlugin(CloudWatchMetricsConfig cfg) {
        return new CloudWatchMetricsPlugin(cfg);
    }

}
