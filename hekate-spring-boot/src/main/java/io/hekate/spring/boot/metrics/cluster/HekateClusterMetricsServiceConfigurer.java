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

package io.hekate.spring.boot.metrics.cluster;

import io.hekate.core.Hekate;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.cluster.ClusterMetricsServiceFactory;
import io.hekate.spring.bean.metrics.ClusterMetricsServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link ClusterMetricsService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link ClusterMetricsServiceFactory} type. This auto-configuration is disabled by
 * default and should be explicitly enabled by setting {@code 'hekate.metrics.cluster.enable'} configuration property to {@code true}.
 * </p>
 *
 *
 * <h2>Configuration properties</h2>
 * <p>
 * It is possible to configure {@link ClusterMetricsServiceFactory} via application properties prefixed with
 * {@code 'hekate.metrics.cluster'} (for example {@link ClusterMetricsServiceFactory#setReplicationInterval(long)
 * 'hekate.metrics.cluster.replication-interval'})
 * </p>
 *
 * @see ClusterMetricsService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(ClusterMetricsServiceFactory.class)
@ConditionalOnProperty(value = "hekate.metrics.cluster.enable", havingValue = "true")
public class HekateClusterMetricsServiceConfigurer {
    /**
     * Constructs the {@link ClusterMetricsServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.metrics.cluster")
    public ClusterMetricsServiceFactory clusterMetricsServiceFactory() {
        return new ClusterMetricsServiceFactory();
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link ClusterMetricsService} directly into other beans instead of
     * accessing it via {@link Hekate#get(Class)} method.
     *
     * @return Service bean.
     */
    @Bean
    public ClusterMetricsServiceBean clusterMetricsService() {
        return new ClusterMetricsServiceBean();
    }
}
