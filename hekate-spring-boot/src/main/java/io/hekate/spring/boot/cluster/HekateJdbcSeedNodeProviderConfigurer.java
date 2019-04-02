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

import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProvider;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import javax.sql.DataSource;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Auto-configuration for {@link JdbcSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.jdbc.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * Note that this auto-configuration requires a {@link Bean} of {@link DataSource} type be defined within the application context. If
 * application uses multiple {@link Bean}s of {@link DataSource} type then one of them must be annotated with {@link Primary} in order to
 * prevent {@link NoUniqueBeanDefinitionException} error.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link JdbcSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link JdbcSeedNodeProviderConfig#setQueryTimeout(int) 'hekate.cluster.seed.jdbc.query-timeout'}</li>
 * <li>{@link JdbcSeedNodeProviderConfig#setCleanupInterval(long) 'hekate.cluster.seed.jdbc.cleanup-interval'}</li>
 * <li>{@link JdbcSeedNodeProviderConfig#setTable(String) 'hekate.cluster.seed.jdbc.table'}</li>
 * <li>{@link JdbcSeedNodeProviderConfig#setClusterColumn(String) 'hekate.cluster.seed.jdbc.cluster-column'}</li>
 * <li>{@link JdbcSeedNodeProviderConfig#setHostColumn(String) 'hekate.cluster.seed.jdbc.host-column'}</li>
 * <li>{@link JdbcSeedNodeProviderConfig#setPortColumn(String) 'hekate.cluster.seed.jdbc.port-column'}</li>
 * </ul>
 *
 * @see HekateClusterServiceConfigurer
 */

@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.jdbc.enable", havingValue = "true")
public class HekateJdbcSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link JdbcSeedNodeProvider} if application doesn't provide its own {@link
     * Bean} of {@link JdbcSeedNodeProviderConfig} type.
     *
     * @param dataSource JDBC data source.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(JdbcSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.jdbc")
    public JdbcSeedNodeProviderConfig jdbcSeedNodeProviderConfig(DataSource dataSource) {
        return new JdbcSeedNodeProviderConfig().withDataSource(dataSource);
    }

    /**
     * Constructs new {@link JdbcSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #jdbcSeedNodeProviderConfig(DataSource)}).
     *
     * @return New provider.
     */
    @Bean
    public JdbcSeedNodeProvider jdbcSeedNodeProvider(JdbcSeedNodeProviderConfig cfg) {
        return new JdbcSeedNodeProvider(cfg);
    }
}
