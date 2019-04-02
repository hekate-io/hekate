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
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.CredentialsSupplier;
import io.hekate.cluster.seed.jclouds.aws.AwsCredentialsSupplier;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link CloudStoreSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.cloudstore.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link CloudStoreSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setProvider(String) 'hekate.cluster.seed.cloudstore.provider'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setContainer(String) 'hekate.cluster.seed.cloudstore.container'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setCredentials(CredentialsSupplier) 'hekate.cluster.seed.cloudstore.identity'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setCredentials(CredentialsSupplier) 'hekate.cluster.seed.cloudstore.credential'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setConnectTimeout(Integer)} 'hekate.cluster.seed.cloudstore.connect-timeout'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setSoTimeout(Integer)} 'hekate.cluster.seed.cloudstore.so-timeout'}</li>
 * <li>{@link CloudStoreSeedNodeProviderConfig#setProperties(Properties) 'hekate.cluster.seed.cloudstore.properties'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.cloudstore.enable", havingValue = "true")
public class HekateCloudStoreSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new credentials supplier based on the provider name. If provider name contains 'aws' then constructs
     * {@link AwsCredentialsSupplier}; otherwise constructs {@link BasicCredentialsSupplier}.
     *
     * @param provider Provider name (see {@link CloudStoreSeedNodeProviderConfig#setProvider(String)}).
     *
     * @return Credentials supplier.
     */
    @Bean
    @ConditionalOnMissingBean(CredentialsSupplier.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.cloudstore")
    public CredentialsSupplier cloudStoreCredentialsSupplier(@Value("${hekate.cluster.seed.cloudstore.provider}") String provider) {
        if (provider.contains("aws")) {
            return new AwsCredentialsSupplier();
        }

        return new BasicCredentialsSupplier();
    }

    /**
     * Conditionally constructs a new configuration for {@link CloudStoreSeedNodeProvider} if application doesn't provide its own {@link
     * Bean} of {@link CloudStoreSeedNodeProviderConfig} type.
     *
     * @param credentials Credentials supplier (see {@link #cloudStoreCredentialsSupplier(String)}).
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(CloudStoreSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.cloudstore")
    public CloudStoreSeedNodeProviderConfig cloudStoreSeedNodeProviderConfig(CredentialsSupplier credentials) {
        return new CloudStoreSeedNodeProviderConfig().withCredentials(credentials);
    }

    /**
     * Constructs new {@link CloudStoreSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #cloudStoreSeedNodeProviderConfig(CredentialsSupplier)}).
     *
     * @return New provider.
     */
    @Bean
    public CloudStoreSeedNodeProvider cloudStoreSeedNodeProvider(CloudStoreSeedNodeProviderConfig cfg) {
        return new CloudStoreSeedNodeProvider(cfg);
    }
}
