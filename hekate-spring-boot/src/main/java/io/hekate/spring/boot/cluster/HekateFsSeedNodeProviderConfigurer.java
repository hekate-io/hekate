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

import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import java.io.File;
import java.io.IOException;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link FsSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.filesystem.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link FsSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link FsSeedNodeProviderConfig#setWorkDir(File) 'hekate.cluster.seed.filesystem.work-dir'}</li>
 * <li>{@link FsSeedNodeProviderConfig#setCleanupInterval(long) 'hekate.cluster.seed.filesystem.cleanup-interval'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.filesystem.enable", havingValue = "true")
public class HekateFsSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link FsSeedNodeProvider} if application doesn't provide its own {@link Bean}
     * of {@link FsSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(FsSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.filesystem")
    public FsSeedNodeProviderConfig fsSeedNodeProviderConfig() {
        return new FsSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link FsSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #fsSeedNodeProviderConfig()}).
     *
     * @return New provider.
     *
     * @throws IOException see {@link FsSeedNodeProvider#FsSeedNodeProvider(FsSeedNodeProviderConfig)}.
     */
    @Bean
    public FsSeedNodeProvider fsSeedNodeProvider(FsSeedNodeProviderConfig cfg) throws IOException {
        return new FsSeedNodeProvider(cfg);
    }
}
