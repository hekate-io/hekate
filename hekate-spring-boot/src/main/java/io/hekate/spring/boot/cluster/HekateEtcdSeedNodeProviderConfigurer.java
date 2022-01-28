/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.cluster.seed.etcd.EtcdSeedNodeProvider;
import io.hekate.cluster.seed.etcd.EtcdSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link EtcdSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.etcd.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link EtcdSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link EtcdSeedNodeProviderConfig#setEndpoints(List) 'hekate.cluster.seed.etcd.endpoints'}</li>
 * <li>{@link EtcdSeedNodeProviderConfig#setUsername(String) 'hekate.cluster.seed.etcd.username'}</li>
 * <li>{@link EtcdSeedNodeProviderConfig#setPassword(String) 'hekate.cluster.seed.etcd.password'}</li>
 * <li>{@link EtcdSeedNodeProviderConfig#setBasePath(String) 'hekate.cluster.seed.etcd.base-path'}</li>
 * <li>{@link EtcdSeedNodeProviderConfig#setCleanupInterval(int) 'hekate.cluster.seed.etcd.cleanup-interval'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.etcd.enable", havingValue = "true")
public class HekateEtcdSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link EtcdSeedNodeProvider} if application doesn't provide its own
     * {@link Bean} of {@link EtcdSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(EtcdSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.etcd")
    public EtcdSeedNodeProviderConfig etcdSeedNodeProviderConfig() {
        return new EtcdSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link EtcdSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #etcdSeedNodeProviderConfig()}).
     *
     * @return New provider.
     */
    @Bean
    public EtcdSeedNodeProvider etcdSeedNodeProvider(EtcdSeedNodeProviderConfig cfg) {
        return new EtcdSeedNodeProvider(cfg);
    }
}
