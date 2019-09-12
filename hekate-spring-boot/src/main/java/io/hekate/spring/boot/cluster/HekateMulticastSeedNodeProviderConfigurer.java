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

import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.net.UnknownHostException;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link MulticastSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.multicast.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link MulticastSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link MulticastSeedNodeProviderConfig#setGroup(String) 'hekate.cluster.seed.multicast.group'}</li>
 * <li>{@link MulticastSeedNodeProviderConfig#setPort(int)  'hekate.cluster.seed.multicast.port'}</li>
 * <li>{@link MulticastSeedNodeProviderConfig#setTtl(int) 'hekate.cluster.seed.multicast.ttl'}</li>
 * <li>{@link MulticastSeedNodeProviderConfig#setInterval(long) 'hekate.cluster.seed.multicast.interval'}</li>
 * <li>{@link MulticastSeedNodeProviderConfig#setWaitTime(long) 'hekate.cluster.seed.multicast.wait-time'}</li>
 * <li>{@link MulticastSeedNodeProviderConfig#setLoopBackDisabled(boolean) 'hekate.cluster.seed.multicast.loop-back-disabled'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.multicast.enable", havingValue = "true")
public class HekateMulticastSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link MulticastSeedNodeProvider} if application doesn't provide its own {@link
     * Bean} of {@link MulticastSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(MulticastSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.multicast")
    public MulticastSeedNodeProviderConfig multicastSeedNodeProviderConfig() {
        return new MulticastSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link MulticastSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #multicastSeedNodeProviderConfig()}).
     *
     * @return New provider.
     *
     * @throws UnknownHostException see {@link MulticastSeedNodeProvider#MulticastSeedNodeProvider(MulticastSeedNodeProviderConfig)}.
     */
    @Bean
    public MulticastSeedNodeProvider multicastSeedNodeProvider(MulticastSeedNodeProviderConfig cfg) throws UnknownHostException {
        return new MulticastSeedNodeProvider(cfg);
    }
}
