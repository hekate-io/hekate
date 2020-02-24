/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.seed.consul.ConsulSeedNodeProvider;
import io.hekate.cluster.seed.consul.ConsulSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link ConsulSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.consul.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link ConsulSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link ConsulSeedNodeProviderConfig#setUrl(String)} 'hekate.cluster.seed.consul.url'}</li>
 * <li>{@link ConsulSeedNodeProviderConfig#setBasePath(String) 'hekate.cluster.seed.consul.base-path'}</li>
 * <li>{@link ConsulSeedNodeProviderConfig#setCleanupInterval(long) 'hekate.cluster.seed.consul.cleanup-interval'}</li>
 * <li>{@link ConsulSeedNodeProviderConfig#setConnectTimeout(Long)} 'hekate.cluster.seed.consul.connect-timeout'}</li>
 * <li>{@link ConsulSeedNodeProviderConfig#setReadTimeout(Long)} 'hekate.cluster.seed.consul.read-timeout'}</li>
 * <li>{@link ConsulSeedNodeProviderConfig#setWriteTimeout(Long)} 'hekate.cluster.seed.consul.write-timeout'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.consul.enable", havingValue = "true")
public class HekateConsulSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link ConsulSeedNodeProvider} if application doesn't provide its own
     * {@link Bean} of {@link ConsulSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(ConsulSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.consul")
    public ConsulSeedNodeProviderConfig consulSeedNodeProviderConfig() {
        return new ConsulSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link ConsulSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #consulSeedNodeProviderConfig()}).
     *
     * @return New provider.
     */
    @Bean
    public ConsulSeedNodeProvider consulSeedNodeProvider(ConsulSeedNodeProviderConfig cfg) {
        return new ConsulSeedNodeProvider(cfg);
    }
}
