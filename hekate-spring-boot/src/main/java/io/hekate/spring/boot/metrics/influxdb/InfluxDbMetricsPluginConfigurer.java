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

package io.hekate.spring.boot.metrics.influxdb;

import io.hekate.metrics.influxdb.InfluxDbMetricsConfig;
import io.hekate.metrics.influxdb.InfluxDbMetricsPlugin;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link InfluxDbMetricsPlugin}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * InfluxDB integration is provided by the 'hekate-metrics-influxdb' module and can be imported into the project dependency management
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
 *   <artifactId>hekate-metrics-influxdb</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-metrics-influxdb', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-metrics-influxdb" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.metrics.influxdb.enable'} property to
 * {@code true} in the application's configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link InfluxDbMetricsPlugin} instance:
 * </p>
 * <ul>
 * <li>{@link InfluxDbMetricsConfig#setUrl(String) 'hekate.metrics.influxdb.url'}</li>
 * <li>{@link InfluxDbMetricsConfig#setUser(String) 'hekate.metrics.influxdb.user'}</li>
 * <li>{@link InfluxDbMetricsConfig#setPassword(String) 'hekate.metrics.influxdb.password'}</li>
 * <li>{@link InfluxDbMetricsConfig#setMaxQueueSize(int) 'hekate.metrics.influxdb.max-queue-size'}</li>
 * <li>{@link InfluxDbMetricsConfig#setTimeout(long) 'hekate.metrics.influxdb.timeout'}</li>
 * </ul>
 *
 * @see InfluxDbMetricsPlugin
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnClass(InfluxDbMetricsPlugin.class)
@ConditionalOnProperty(value = "hekate.metrics.influxdb.enable", havingValue = "true")
public class InfluxDbMetricsPluginConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link InfluxDbMetricsPlugin} if application doesn't provide its own {@link Bean} of
     * {@link InfluxDbMetricsConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(InfluxDbMetricsConfig.class)
    @ConfigurationProperties(prefix = "hekate.metrics.influxdb")
    public InfluxDbMetricsConfig influxDbMetricsConfig() {
        return new InfluxDbMetricsConfig();
    }

    /**
     * Constructs new {@link InfluxDbMetricsPlugin}.
     *
     * @param cfg Configuration (see {@link #influxDbMetricsConfig()}).
     *
     * @return New plugin.
     */
    @Bean
    public InfluxDbMetricsPlugin influxDbMetricsPlugin(InfluxDbMetricsConfig cfg) {
        return new InfluxDbMetricsPlugin(cfg);
    }
}
