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

import io.hekate.cluster.seed.jclouds.BasicCredentialsSupplier;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.CredentialsSupplier;
import io.hekate.cluster.seed.jclouds.aws.AwsCredentialsSupplier;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link CloudSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.cloud.enable'} property to
 * {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link CloudSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link CloudSeedNodeProviderConfig#setProvider(String) 'hekate.cluster.seed.cloud.provider'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setEndpoint(String) 'hekate.cluster.seed.cloud.endpoint'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setCredentials(CredentialsSupplier) 'hekate.cluster.seed.cloud.identity'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setCredentials(CredentialsSupplier) 'hekate.cluster.seed.cloud.credential'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setRegions(Set) 'hekate.cluster.seed.cloud.regions'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setZones(Set) 'hekate.cluster.seed.cloud.zones'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setTags(Map) 'hekate.cluster.seed.cloud.tags'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setConnectTimeout(Integer)} 'hekate.cluster.seed.cloud.connect-timeout'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setSoTimeout(Integer)} 'hekate.cluster.seed.cloud.so-timeout'}</li>
 * <li>{@link CloudSeedNodeProviderConfig#setProperties(Properties) 'hekate.cluster.seed.cloud.properties'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.cloud.enable", havingValue = "true")
public class HekateCloudSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new credentials supplier based on the provider name. If provider name contains 'aws' then constructs
     * {@link AwsCredentialsSupplier}; otherwise constructs {@link BasicCredentialsSupplier}.
     *
     * @param provider Provider name (see {@link CloudSeedNodeProviderConfig#setProvider(String)}).
     *
     * @return Credentials supplier.
     */
    @Bean
    @ConditionalOnMissingBean(CredentialsSupplier.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.cloud")
    public CredentialsSupplier cloudCredentialsSupplier(@Value("${hekate.cluster.seed.cloud.provider}") String provider) {
        if (provider.contains("aws")) {
            return new AwsCredentialsSupplier();
        }

        return new BasicCredentialsSupplier();
    }

    /**
     * Conditionally constructs a new configuration for {@link CloudSeedNodeProvider} if application doesn't provide its own {@link Bean} of
     * {@link CloudSeedNodeProviderConfig} type.
     *
     * @param credentials Credentials supplier (see {@link #cloudCredentialsSupplier(String)}).
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(CloudSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.cloud")
    public CloudSeedNodeProviderConfig cloudSeedNodeProviderConfig(CredentialsSupplier credentials) {
        return new CloudSeedNodeProviderConfig().withCredentials(credentials);
    }

    /**
     * Constructs new {@link CloudSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #cloudSeedNodeProviderConfig(CredentialsSupplier)}).
     *
     * @return New provider.
     */
    @Bean
    public CloudSeedNodeProvider cloudSeedNodeProvider(CloudSeedNodeProviderConfig cfg) {
        return new CloudSeedNodeProvider(cfg);
    }
}
