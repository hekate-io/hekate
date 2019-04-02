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

import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.net.UnknownHostException;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link StaticSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.static.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The list of seed node addresses can be specified via {@link StaticSeedNodeProviderConfig#setAddresses(List)
 * 'hekate.cluster.seed.static.addresses'} configuration option.
 * </p>
 *
 * @see HekateClusterServiceConfigurer
 */

@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.static.enable", havingValue = "true")
public class HekateStaticSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link StaticSeedNodeProvider} if application doesn't provide its own {@link
     * Bean} of {@link StaticSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(StaticSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.static")
    public StaticSeedNodeProviderConfig staticSeedNodeProviderConfig() {
        return new StaticSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link StaticSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #staticSeedNodeProviderConfig()}).
     *
     * @return New provider.
     *
     * @throws UnknownHostException see {@link StaticSeedNodeProvider#StaticSeedNodeProvider(StaticSeedNodeProviderConfig)}.
     */
    @Bean
    public StaticSeedNodeProvider staticSeedNodeProvider(StaticSeedNodeProviderConfig cfg) throws UnknownHostException {
        return new StaticSeedNodeProvider(cfg);
    }
}
