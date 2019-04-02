/*
 * Copyright 2019 The Hekate Project
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

import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProvider;
import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link ZooKeeperSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.zookeeper.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link ZooKeeperSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link ZooKeeperSeedNodeProviderConfig#setConnectionString(String)} 'hekate.cluster.seed.zookeeper.connection-string'}</li>
 * <li>{@link ZooKeeperSeedNodeProviderConfig#setBasePath(String)} 'hekate.cluster.seed.zookeeper.base-path'}</li>
 * <li>{@link ZooKeeperSeedNodeProviderConfig#setConnectTimeout(int)} 'hekate.cluster.seed.zookeeper.connect-timeout'}</li>
 * <li>{@link ZooKeeperSeedNodeProviderConfig#setSessionTimeout(int)} 'hekate.cluster.seed.zookeeper.session-timeout'}</li>
 * <li>{@link ZooKeeperSeedNodeProviderConfig#setCleanupInterval(int)} 'hekate.cluster.seed.zookeeper.cleanup-interval'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.zookeeper.enable", havingValue = "true")
public class HekateZooKeeperSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link ZooKeeperSeedNodeProvider} if application doesn't provide its own
     * {@link Bean} of {@link ZooKeeperSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(ZooKeeperSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.zookeeper")
    public ZooKeeperSeedNodeProviderConfig zooKeeperSeedNodeProviderConfig() {
        return new ZooKeeperSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link ZooKeeperSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #zooKeeperSeedNodeProviderConfig()}).
     *
     * @return New provider.
     */
    @Bean
    public ZooKeeperSeedNodeProvider fsSeedNodeProvider(ZooKeeperSeedNodeProviderConfig cfg) {
        return new ZooKeeperSeedNodeProvider(cfg);
    }
}
